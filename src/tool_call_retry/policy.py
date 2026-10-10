"""Retry policy: exponential backoff with jitter, attempt and time budgets.

Reusable on its own — it does not know anything about sagas. Backoff follows the
AWS "full jitter" recipe: the uncapped window is ``base_delay * multiplier ** (attempt - 1)``,
the window itself is capped at ``max_delay`` and the actual sleep is drawn from
``[0, window]`` for ``jitter="full"``.

The policy deliberately does **not** sleep inside :meth:`RetryPolicy.run` unless
a sleep function is provided; tests and the saga runtime pass their own so no
test suite ever waits on wall-clock time.
"""

from __future__ import annotations

import asyncio
import builtins
import math
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from tool_call_retry.errors import (
    HTTPStatusError,
    NonRetryableError,
    RetryableError,
    RetryExhausted,
    ToolCallError,
)

#: Exceptions treated as transient unless ``retryable_exceptions`` overrides them.
DEFAULT_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    TimeoutError,  # socket.timeout and asyncio timeouts are aliases/subclasses
    ConnectionError,  # includes ConnectionResetError, ConnectionRefusedError, ...
    RetryableError,
    ToolCallError,
)

JITTER_STRATEGIES = ("full", "sequential", "none")


def _notify(
    on_attempt: Callable[[Any], Any] | None,
    record: Any,
) -> None:
    """Invoke the observer without letting it mask the real failure."""
    if on_attempt is None:
        return
    try:
        on_attempt(record)
    except Exception as notify_exc:  # never BaseException: don't swallow Ctrl-C
        note = f" (on_attempt failed: {notify_exc!r})"
        record.error = f"{record.error}{note}" if record.error else note.strip()


@dataclass(frozen=True)
class RetryPolicy:
    """How many times to retry, how long to wait, and what is worth retrying.

    Args:
        max_attempts: Total attempts including the first call.
        base_delay: Seconds for the first backoff window.
        multiplier: Growth factor of the window per attempt.
        max_delay: Upper bound of a single backoff window.
        jitter: ``full`` (uniform in [0, window]), ``sequential`` (uniform in
            [window/2, window]) or ``none`` (always the full window).
        max_total_delay: Wall-clock budget for all sleeps. Once the budget is
            spent the policy stops even if attempts remain.
        retryable_exceptions: Exception types (or ``(exc, predicate)`` pairs) that
            justify another attempt. Replaces the defaults when given.
        sleep: Default sleep function; defaults to :func:`time.sleep` /
            :func:`asyncio.sleep`.
        rng: Random source, injectable for deterministic tests.
    """

    max_attempts: int = 3
    base_delay: float = 0.1
    multiplier: float = 2.0
    max_delay: float = 30.0
    jitter: str = "full"
    max_total_delay: float = 60.0
    retryable_exceptions: tuple[Any, ...] = DEFAULT_RETRYABLE_EXCEPTIONS
    sleep: Callable[[float], Any] | None = None
    rng: random.Random | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if self.max_delay < 0:
            raise ValueError("max_delay must be >= 0")
        if self.max_total_delay < 0:
            raise ValueError("max_total_delay must be >= 0")
        if self.jitter not in JITTER_STRATEGIES:
            raise ValueError(
                f"jitter must be one of {JITTER_STRATEGIES}, got {self.jitter!r}"
            )

    # -- backoff ----------------------------------------------------------
    def backoff_window(self, attempt: int) -> float:
        """Un-jittered, capped window for ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        if self.base_delay <= 0 or self.max_delay <= 0:
            return 0.0
        if not math.isfinite(self.max_delay):
            return float('inf')
        if not math.isfinite(self.multiplier):
            return self.base_delay if attempt == 1 else self.max_delay
        if self.multiplier == 1.0:
            return builtins.min(self.base_delay, self.max_delay)
        # Compare in log space so a large attempt count can never overflow:
        # past this exponent the series is already >= max_delay and the cap wins.
        cap_exponent = math.log(self.max_delay / self.base_delay) / math.log(self.multiplier)
        if attempt - 1 >= cap_exponent:
            return self.max_delay
        return builtins.min(self.base_delay * (self.multiplier ** (attempt - 1)), self.max_delay)

    def delay_for(self, attempt: int, *, rng: random.Random | None = None) -> float:
        """Seconds to sleep before ``attempt``, including jitter."""
        window = self.backoff_window(attempt)
        if self.jitter == "none" or window == 0:
            return window
        source = rng or self.rng or random
        if self.jitter == "full":
            return source.uniform(0.0, window)
        return source.uniform(window / 2.0, window)

    # -- classification ---------------------------------------------------
    def is_retryable(
        self, exc: BaseException, *, attempt: int | None = None
    ) -> bool:
        """Whether ``exc`` justifies another attempt.

        HTTP-shaped errors are classified by status: 429 and 5xx are transient,
        every other 4xx is terminal. Explicit ``NonRetryableError`` and
        ``ToolCallError(retryable=False)`` always win over type membership.
        """
        if attempt is not None and attempt >= self.max_attempts:
            return False
        # HTTP status must be checked before NonRetryableError: an HTTPStatusError
        # subclasses NonRetryableError to be a *default*, but 429/5xx are transient.
        if isinstance(exc, HTTPStatusError):
            return bool(exc.retryable)
        if isinstance(exc, NonRetryableError):
            return False
        if isinstance(exc, ToolCallError):
            return bool(exc.retryable)
        if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
            return False
        return self._matches_retryable_spec(exc)

    def _matches_retryable_spec(self, exc: BaseException) -> bool:
        spec = self.retryable_exceptions
        for entry in spec:
            if isinstance(entry, tuple):
                exc_type, predicate = entry
                if isinstance(exc, exc_type) and predicate(exc):
                    return True
            elif isinstance(entry, type) and isinstance(exc, entry):
                return True
            elif callable(entry) and not isinstance(entry, type):
                try:
                    if entry(exc):
                        return True
                except Exception:  # pragma: no cover - defensive
                    continue
        return False

    # -- execution --------------------------------------------------------
    def run(
        self,
        func: Callable[..., Any],
        *args: Any,
        sleep: Callable[[float], Any] | None = None,
        on_attempt: Callable[[Any], Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Call ``func`` with retries until it succeeds or the budget is spent.

        Raises:
            RetryExhausted: the budget ran out while the last error was retryable.
            BaseException: whatever ``func`` raised on its final attempt when the
                error was terminal (never wrapped).
        """
        sleeper = sleep or self.sleep or _time_sleep
        history: list[Any] = []
        spent = 0.0
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                from tool_call_retry.models import RetryAttempt

                record = RetryAttempt(
                    step_id=0,
                    attempt=attempt,
                    error=f"{type(exc).__name__}: {exc}",
                    succeeded=False,
                )
                history.append(record)
                if not self.is_retryable(exc):
                    _notify(on_attempt, record)
                    raise
                delay = self.delay_for(attempt)
                record.delay = delay
                _notify(on_attempt, record)
                if attempt >= self.max_attempts or spent + delay > self.max_total_delay:
                    raise RetryExhausted(attempt, exc, history) from exc
                spent += delay
                try:
                    sleeper(delay)
                except Exception as sleep_exc:
                    record.error = f"{record.error} (sleep failed: {sleep_exc!r})"
            else:
                from tool_call_retry.models import RetryAttempt

                record = RetryAttempt(
                    step_id=0, attempt=attempt, delay=0.0, succeeded=True
                )
                history.append(record)
                _notify(on_attempt, record)
                return result
        raise AssertionError("unreachable")  # pragma: no cover

    async def arun(
        self,
        func: Callable[..., Awaitable[Any]],
        *args: Any,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
        on_attempt: Callable[[Any], Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Async twin of :meth:`run`. ``func`` may be sync or async."""
        sleeper = sleep or self.sleep or asyncio.sleep
        history: list[Any] = []
        spent = 0.0
        for attempt in range(1, self.max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
                    result = await result
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                from tool_call_retry.models import RetryAttempt

                record = RetryAttempt(
                    step_id=0,
                    attempt=attempt,
                    error=f"{type(exc).__name__}: {exc}",
                    succeeded=False,
                )
                history.append(record)
                if not self.is_retryable(exc):
                    _notify(on_attempt, record)
                    raise
                delay = self.delay_for(attempt)
                record.delay = delay
                _notify(on_attempt, record)
                if attempt >= self.max_attempts or spent + delay > self.max_total_delay:
                    raise RetryExhausted(attempt, exc, history) from exc
                spent += delay
                try:
                    await _maybe_await(sleeper(delay))
                except Exception as sleep_exc:
                    record.error = f"{record.error} (sleep failed: {sleep_exc!r})"
            else:
                from tool_call_retry.models import RetryAttempt

                record = RetryAttempt(
                    step_id=0, attempt=attempt, delay=0.0, succeeded=True
                )
                history.append(record)
                _notify(on_attempt, record)
                return result
        raise AssertionError("unreachable")  # pragma: no cover

    # -- serialization ----------------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "max_attempts": self.max_attempts,
            "base_delay": self.base_delay,
            "multiplier": self.multiplier,
            "max_delay": self.max_delay,
            "jitter": self.jitter,
            "max_total_delay": self.max_total_delay,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> RetryPolicy:
        known = {
            key: payload[key]
            for key in (
                "max_attempts",
                "base_delay",
                "multiplier",
                "max_delay",
                "jitter",
                "max_total_delay",
            )
            if key in payload
        }
        return cls(**known)

    @classmethod
    def from_yaml(cls, path: str | Path) -> RetryPolicy:
        """Load a policy from a YAML file (see issue #4: configurable via YAML).

        Accepts either a flat mapping or a ``retry:``-nested one.
        """
        import yaml

        data = yaml.safe_load(Path(path).read_text()) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{path}: expected a YAML mapping, got {type(data).__name__}")
        payload = data.get("retry", data)
        if not isinstance(payload, dict):
            raise ValueError(f"{path}: 'retry' must be a mapping")
        return cls.from_dict(payload)

    def with_overrides(self, **kwargs: Any) -> RetryPolicy:
        return replace(self, **kwargs)


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, Awaitable):
        return await value
    return value


def _time_sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)


def default_retryable_types() -> Sequence[type[BaseException]]:
    """The default retryable exception types, for introspection/documentation."""
    return DEFAULT_RETRYABLE_EXCEPTIONS


__all__ = [
    "DEFAULT_RETRYABLE_EXCEPTIONS",
    "JITTER_STRATEGIES",
    "RetryPolicy",
    "default_retryable_types",
]

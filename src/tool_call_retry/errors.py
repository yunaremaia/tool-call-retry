"""Exception hierarchy for tool-call-retry.

The split between :class:`RetryableError` and :class:`NonRetryableError` is the
contract the retry policy uses to decide whether another attempt is worthwhile.
:class:`HTTPStatusError` encodes the usual provider rule: 429 and 5xx are
transient, every other 4xx is the caller's fault and will never succeed on a
retry.
"""

from __future__ import annotations


class ToolCallRetryError(Exception):
    """Base class for every error raised by this package."""


class InvalidTransition(ToolCallRetryError):
    """Raised when a saga or step is moved into a state it cannot legally reach."""


class RetryableError(ToolCallRetryError):
    """A transient failure: retrying the same call may succeed."""


class NonRetryableError(ToolCallRetryError):
    """A terminal failure: retrying the same call cannot help."""


class ToolCallError(RetryableError):
    """A tool call failed; ``retryable=False`` marks it as terminal."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"{type(self).__name__}({str(self)!r}, retryable={self.retryable})"


class HTTPStatusError(NonRetryableError):
    """An HTTP-shaped failure.

    Retryable when the status is 429 (rate limited) or any 5xx (server side).
    Every other status, including 4xx client errors, is terminal.
    """

    #: Statuses always treated as transient regardless of subclass overrides.
    RETRYABLE_STATUSES = frozenset({408, 409, 425, 429})

    def __init__(self, status_code: int, message: str = "", *, body: str | None = None) -> None:
        self.status_code = int(status_code)
        self.body = body
        super().__init__(message or f"HTTP {self.status_code}")

    @property
    def retryable(self) -> bool:  # type: ignore[override]
        if self.status_code in self.RETRYABLE_STATUSES:
            return True
        return 500 <= self.status_code < 600

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"HTTPStatusError({self.status_code}, {str(self)!r})"


class SagaFailed(ToolCallRetryError):
    """A saga stopped on a terminal step failure after compensating what it could."""

    def __init__(
        self,
        message: str,
        *,
        saga_id: str,
        failed_step: str | None = None,
        error: BaseException | None = None,
        completed: list[str] | None = None,
        compensated: list[str] | None = None,
        compensation_errors: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.saga_id = saga_id
        self.failed_step = failed_step
        self.error = error
        self.completed = list(completed or [])
        self.compensated = list(compensated or [])
        self.compensation_errors = dict(compensation_errors or {})

    def root_cause(self) -> BaseException | None:
        """The exception an agent should actually react to.

        Retries wrap the real failure, so a saga that ran out of attempts
        reports the underlying ``TimeoutError``/``HTTPStatusError`` rather than
        the retry bookkeeping.
        """
        from tool_call_retry.errors import RetryExhausted

        if isinstance(self.error, RetryExhausted):
            return self.error.last_error
        return self.error

    def summary(self) -> str:
        """Human-readable state of the system after the saga gave up."""
        stuck = list(self.compensation_errors)
        if self.compensated and not stuck:
            rolled = f"steps {', '.join(self.compensated)} compensated"
            state = "System state is consistent."
        elif not self.compensated and not self.completed:
            rolled = "nothing to compensate"
            state = "System state is unchanged."
        else:
            # A step whose undo raised stays mid-compensation with its side
            # effect still applied, so the system is inconsistent no matter how
            # many *other* steps unwound cleanly. `completed` is the fallback for
            # a completed step that had no undo registered at all (#11).
            names = stuck or list(self.completed)
            rolled = f"compensation incomplete for steps {', '.join(names)}"
            if self.compensated:
                rolled = f"steps {', '.join(self.compensated)} compensated, {rolled}"
            state = "System state may be inconsistent; manual cleanup required."
        done = (
            f"steps {', '.join(self.completed)} succeeded"
            if self.completed
            else "no steps succeeded"
        )
        cause = self.root_cause()
        reason = (
            f"step {self.failed_step} failed ({type(cause).__name__})"
            if self.failed_step
            else "saga failed"
        )
        return f"{done}, {reason}, {rolled}. {state}"


class RetryExhausted(ToolCallRetryError):
    """All attempts were consumed and the last failure was itself retryable."""

    def __init__(
        self,
        attempts: int,
        last_error: BaseException,
        attempt_history: list | None = None,
    ) -> None:
        super().__init__(f"{attempts} attempts exhausted: {last_error!r}")
        self.attempts = attempts
        self.last_error = last_error
        self.attempt_history = list(attempt_history or [])

    def summary(self) -> str:
        return (
            f"Failed after {self.attempts} attempts. "
            f"Last error: {type(self.last_error).__name__}: {self.last_error}"
        )


__all__ = [
    "HTTPStatusError",
    "InvalidTransition",
    "NonRetryableError",
    "RetryExhausted",
    "RetryableError",
    "SagaFailed",
    "ToolCallError",
    "ToolCallRetryError",
]

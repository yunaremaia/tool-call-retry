"""Saga runtime: run tool calls in order, roll back on failure, resume after crashes.

A saga is an ordered list of steps. Each step has a ``do`` callable and an
optional ``compensate`` callable. Steps run sequentially; the first terminal
failure stops the saga and every *completed* step is compensated in reverse
order. Transient failures are retried by the injected
:class:`~tool_call_retry.policy.RetryPolicy` before being treated as terminal.

Idempotency comes from the journal: a step that is already recorded as
``completed`` is not executed again, so re-running a saga after a crash only
re-runs what never finished.
"""

from __future__ import annotations

import inspect
import uuid
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any

import asyncio

from tool_call_retry.errors import NonRetryableError, SagaFailed
from tool_call_retry.journal import SagaJournal
from tool_call_retry.models import RetryAttempt, SagaRun, StepStatus, ToolCall
from tool_call_retry.policy import RetryPolicy, _time_sleep


@dataclass
class SagaStep:
    """One tool call plus its undo."""

    name: str
    do: Callable[..., Any]
    compensate: Callable[..., Any] | None = None
    tool_args: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str | None = None
    max_attempts: int | None = None

    @property
    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.do)


class Saga:
    """Orchestrates steps, compensation and (with a journal) crash recovery.

    Args:
        name: Human-readable saga name.
        journal: Persistence layer. Without one the saga runs in memory only and
            cannot be resumed.
        saga_id: Explicit id for the saga. Ignored when ``idempotency_key`` is
            given and a saga already exists for that key.
        idempotency_key: Re-running with the same key resumes the existing saga
            instead of duplicating its side effects.
        policy: Retry policy for transient step failures.
        policy_kwargs: Overrides used to build a default policy.
        sleep: Injectable sleep so tests never wait on wall-clock time.

    Example:
        >>> saga = Saga(name="checkout", journal=SagaJournal(":memory:"))
        >>> @saga.tool("charge", compensate=lambda **kw: "refunded")
        ... def charge():
        ...     return {"charge_id": "ch_1"}
        >>> saga.execute().status
        'completed'
    """

    def __init__(
        self,
        name: str,
        *,
        journal: SagaJournal | None = None,
        saga_id: str | None = None,
        idempotency_key: str | None = None,
        policy: RetryPolicy | None = None,
        policy_kwargs: dict[str, Any] | None = None,
        sleep: Callable[[float], Any] | None = None,
    ) -> None:
        self.name = name
        self.journal = journal
        self.saga_id = saga_id
        self.idempotency_key = idempotency_key
        self.policy = policy or RetryPolicy(**(policy_kwargs or {}))
        self.sleep = sleep
        self.steps: list[SagaStep] = []
        #: Set when a step was retried, for observability.
        self.last_attempts: list[RetryAttempt] = []

    # -- registration -----------------------------------------------------
    def tool(
        self,
        name: str,
        *,
        compensate: Callable[..., Any] | None = None,
        args: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
        **kwargs: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator registering ``func`` as a saga step.

        Keyword arguments are bound as the step's tool arguments, so
        ``@saga.tool("charge", amount=10)`` calls ``do(amount=10)``. Pass ``args``
        for the same effect, and ``compensate=`` for the undo.

        The decorated function is returned unchanged so it stays directly
        callable. Async functions are supported (``execute`` rejects them; use
        :meth:`aexecute`).
        """
        bound = dict(args or {})
        bound.update(kwargs)

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.add_step(
                name,
                func,
                compensate=compensate,
                tool_args=bound,
                idempotency_key=idempotency_key,
                max_attempts=max_attempts,
            )
            return func

        return decorator

    def add_step(
        self,
        name: str,
        do: Callable[..., Any],
        *,
        compensate: Callable[..., Any] | None = None,
        tool_args: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
    ) -> SagaStep:
        if any(step.name == name for step in self.steps):
            raise ValueError(f"duplicate step name: {name!r}")
        step = SagaStep(
            name=name,
            do=do,
            compensate=compensate,
            tool_args=dict(tool_args or {}),
            idempotency_key=idempotency_key,
            max_attempts=max_attempts,
        )
        self.steps.append(step)
        return step

    # -- execution --------------------------------------------------------
    def execute(self, **context: Any) -> SagaRun:
        """Run every step synchronously, compensating on terminal failure."""
        self._reject_async_steps()
        run, resumed = self._start()
        for step in self._pending(run, resumed):
            self._ensure_journalled(run, step)
            try:
                result = self._run_step_sync(step, run, context)
                self._succeed(run, step, result)
            except BaseException as exc:  # noqa: BLE001 - reported as SagaFailed
                self._fail(run, step, exc, context)
        return self._finish(run)

    async def aexecute(self, **context: Any) -> SagaRun:
        """Async twin of :meth:`execute`; sync and async steps may be mixed.

        An ``async def`` compensation is awaited here, so it really runs before
        the step is reported as compensated.
        """
        run, resumed = self._start()
        for step in self._pending(run, resumed):
            self._ensure_journalled(run, step)
            try:
                result = await self._run_step_async(step, run, context)
                self._succeed(run, step, result)
            except BaseException as exc:  # noqa: BLE001 - reported as SagaFailed
                await self._afail(run, step, exc, context)
        return self._finish(run)

    def _reject_async_steps(self) -> None:
        async_steps = [s.name for s in self.steps if s.is_async]
        if async_steps:
            raise ValueError(
                "async steps require aexecute(): " + ", ".join(async_steps)
            )

    def _start(self) -> tuple[SagaRun, bool]:
        if not self.steps:
            raise ValueError("cannot execute a saga with no steps")
        return self._prepare()

    def _pending(self, run: SagaRun, resumed: bool) -> list[SagaStep]:
        """Steps to run now; already-completed ones are skipped when resuming."""
        if not resumed:
            return list(self.steps)
        return [s for s in self.steps if not self._already_done(run, s)]

    def _succeed(self, run: SagaRun, step: SagaStep, result: Any) -> None:
        step_id = run.step_by_name(step.name).step_id
        # Journal before transitioning in memory: a refused write must leave the
        # step in ``running`` (from which FAILED is legal) so the caller's
        # compensation path can mark it failed instead of raising InvalidTransition.
        if self.journal is not None:
            self.journal.mark_step_completed(run.saga_id, step_id, result)
        run.transition_step(step_id, StepStatus.COMPLETED, result=result)

    def _fail(
        self, run: SagaRun, step: SagaStep, failure: BaseException, context: dict[str, Any]
    ) -> None:
        """Mark the step failed, roll back, persist, then raise :class:`SagaFailed`."""
        completed = self._mark_step_failed(run, step, failure)
        self._raise_saga_failed(
            run, step, failure, completed, *self._compensate(run, context)
        )

    async def _afail(
        self, run: SagaRun, step: SagaStep, failure: BaseException, context: dict[str, Any]
    ) -> None:
        """Async twin of :meth:`_fail`; awaits an ``async def`` compensation."""
        completed = self._mark_step_failed(run, step, failure)
        self._raise_saga_failed(
            run, step, failure, completed, *await self._acompensate(run, context)
        )

    def _mark_step_failed(
        self, run: SagaRun, step: SagaStep, failure: BaseException
    ) -> list[str]:
        """Record the terminal failure; return the completed steps to roll back."""
        step_id = run.step_by_name(step.name).step_id
        message = f"{type(failure).__name__}: {failure}"
        run.transition_step(step_id, StepStatus.FAILED, error=message)
        if self.journal is not None:
            self.journal.mark_step_failed(run.saga_id, step_id, message)
        # Snapshot the completed set *before* compensating rewrites those statuses,
        # so the reported error reflects what actually ran.
        return run.completed_steps

    def _raise_saga_failed(
        self,
        run: SagaRun,
        step: SagaStep,
        failure: BaseException,
        completed: list[str],
        compensated: list[str],
        comp_errors: dict[str, str],
    ) -> None:
        """Close the saga out as failed and raise the LLM-facing report."""
        run.transition_saga_status("failed")
        if self.journal is not None:
            self.journal.mark_saga_failed(run.saga_id)
        raise SagaFailed(
            f"saga {run.saga_id!r} failed at step {step.name!r}: {failure}",
            saga_id=run.saga_id,
            failed_step=step.name,
            error=failure,
            completed=completed,
            compensated=compensated,
            compensation_errors=comp_errors,
        ) from failure

    def _finish(self, run: SagaRun) -> SagaRun:
        run.transition_saga_status("completed")
        if self.journal is not None:
            self.journal.mark_saga_completed(run.saga_id)
        return run

    def _prepare(self) -> tuple[SagaRun, bool]:
        """Load or create the run; report whether this is a resume."""
        journal = self.journal
        if journal is None:
            if self.idempotency_key:
                raise ValueError("a journal is required for idempotent resume")
            run = SagaRun(saga_id=self.saga_id or f"saga-{uuid.uuid4().hex[:12]}", name=self.name)
            for step in self.steps:
                run.add_step(step.name, step.tool_args)
            return run, False

        existing = journal.find_saga_by_key(self.idempotency_key) if self.idempotency_key else None
        if existing and not self.saga_id:
            run = journal.load_saga(existing)
            self.saga_id = run.saga_id
            # A saga that was compensating/failed when the process stopped is
            # being retried: its undone steps must be run again from scratch.
            if run.status in ("compensating", "failed"):
                journal.reset_for_retry(existing)
                journal.reopen_saga(existing)
                run = journal.load_saga(existing)
            else:
                journal.resume_saga(existing)
            for step in self.steps:
                self._ensure_journalled(run, step)
            # An existing saga for this key always counts as a resume, even when
            # everything it had recorded so far is already done.
            return run, True

        saga_id = journal.begin_saga(
            self.saga_id, idempotency_key=self.idempotency_key, name=self.name
        )
        run = journal.load_saga(saga_id)
        for step in self.steps:
            self._ensure_journalled(run, step)
        return run, False

    def _ensure_journalled(self, run: SagaRun, step: SagaStep) -> None:
        if self.journal is None:
            return
        if run.step_by_name_safe(step.name) is not None:
            return
        call = run.add_step(step.name, step.tool_args)
        if step.idempotency_key:
            call.idempotency_key = step.idempotency_key
        self.journal.record_step(
            run.saga_id,
            call.step_id,
            step.name,
            step.tool_args,
            idempotency_key=step.idempotency_key,
        )

    def _already_done(self, run: SagaRun, step: SagaStep) -> bool:
        call = run.step_by_name_safe(step.name)
        return call is not None and call.status is StepStatus.COMPLETED

    # -- step execution ---------------------------------------------------
    def _policy_for(self, step: SagaStep) -> RetryPolicy:
        if step.max_attempts is None:
            return self.policy
        return self.policy.with_overrides(max_attempts=step.max_attempts)

    def _run_step_sync(self, step: SagaStep, run: SagaRun, context: dict[str, Any]) -> Any:
        call = run.step_by_name(step.name)
        run.transition_step(call.step_id, StepStatus.RUNNING)
        if self.journal is not None:
            self.journal.mark_step_running(run.saga_id, call.step_id)
        return self._invoke_with_retry(step, run, call.step_id, context)

    async def _run_step_async(
        self, step: SagaStep, run: SagaRun, context: dict[str, Any]
    ) -> Any:
        call = run.step_by_name(step.name)
        run.transition_step(call.step_id, StepStatus.RUNNING)
        if self.journal is not None:
            self.journal.mark_step_running(run.saga_id, call.step_id)
        return await self._ainvoke_with_retry(step, run, call.step_id, context)

    def _invoke_with_retry(
        self, step: SagaStep, run: SagaRun, step_id: int, context: dict[str, Any]
    ) -> Any:
        policy = self._policy_for(step)
        sleeper = self.sleep or _time_sleep

        def on_attempt(attempt: RetryAttempt) -> None:
            record = RetryAttempt(
                step_id=step_id,
                attempt=attempt.attempt,
                delay=attempt.delay,
                error=attempt.error,
                succeeded=attempt.succeeded,
            )
            self.last_attempts.append(record)
            run.transition_step(step_id, StepStatus.RUNNING, attempt=record)
            if self.journal is not None:
                self.journal.record_attempt(
                    run.saga_id,
                    step_id,
                    attempt=record.attempt,
                    error=record.error,
                    delay=record.delay,
                    succeeded=record.succeeded,
                )

        return policy.run(
            lambda: step.do(**step.tool_args, **context),
            sleep=sleeper,
            on_attempt=on_attempt,
        )

    async def _ainvoke_with_retry(
        self, step: SagaStep, run: SagaRun, step_id: int, context: dict[str, Any]
    ) -> Any:
        policy = self._policy_for(step)
        sleeper = self.sleep

        async def no_sleep(_delay: float) -> None:
            if sleeper is not None:
                sleeper(_delay)
            else:
                await asyncio.sleep(_delay)

        def on_attempt(attempt: RetryAttempt) -> None:
            record = RetryAttempt(
                step_id=step_id,
                attempt=attempt.attempt,
                delay=attempt.delay,
                error=attempt.error,
                succeeded=attempt.succeeded,
            )
            self.last_attempts.append(record)
            run.transition_step(step_id, StepStatus.RUNNING, attempt=record)
            if self.journal is not None:
                self.journal.record_attempt(
                    run.saga_id,
                    step_id,
                    attempt=record.attempt,
                    error=record.error,
                    delay=record.delay,
                    succeeded=record.succeeded,
                )

        return await policy.arun(
            lambda: step.do(**step.tool_args, **context),
            sleep=no_sleep,
            on_attempt=on_attempt,
        )

    # -- compensation -----------------------------------------------------
    def _compensate(
        self, run: SagaRun, context: dict[str, Any]
    ) -> tuple[list[str], dict[str, str]]:
        """Roll back completed steps in reverse order.

        Returns:
            ``(compensated_step_names, errors_by_step_name)``. A compensation that
            raises does not stop the remaining ones and is reported to the caller.
        """
        return self._rollback(
            run, lambda undo, name, result: self._call_compensation(undo, name, result, context)
        )

    async def _acompensate(
        self, run: SagaRun, context: dict[str, Any]
    ) -> tuple[list[str], dict[str, str]]:
        """Async twin of :meth:`_compensate`; awaits an ``async def`` undo."""
        return await self._arollback(
            run,
            lambda undo, name, result: self._acall_compensation(undo, name, result, context),
        )

    def _rollback(
        self,
        run: SagaRun,
        invoke: Callable[[Callable[..., Any], str, Any], Any],
    ) -> tuple[list[str], dict[str, str]]:
        compensated, errors = self._begin_rollback(run)
        for name in run.completed_steps_to_compensate:
            undo, call = self._begin_step_rollback(run, name, errors)
            if undo is None or call is None:
                continue
            try:
                result = invoke(undo, name, call.result)
                # Journalling the result is inside the try: a write failure must not
                # mask the real SagaFailed nor abort the remaining undos (#23).
                self._end_step_rollback(run, call, result, compensated)
            except BaseException as exc:  # noqa: BLE001 - reported, never swallowed
                self._end_step_rollback_failed(run, call, errors, exc)
        return compensated, errors

    async def _arollback(
        self,
        run: SagaRun,
        invoke: Callable[[Callable[..., Any], str, Any], Any],
    ) -> tuple[list[str], dict[str, str]]:
        """Async twin of :meth:`_rollback`; ``invoke`` may return an awaitable."""
        compensated, errors = self._begin_rollback(run)
        for name in run.completed_steps_to_compensate:
            undo, call = self._begin_step_rollback(run, name, errors)
            if undo is None or call is None:
                continue
            try:
                result = await self._resolve(invoke(undo, name, call.result))
                # Journalling the result is inside the try, as on the sync path (#23).
                self._end_step_rollback(run, call, result, compensated)
            except BaseException as exc:  # noqa: BLE001 - reported, never swallowed
                self._end_step_rollback_failed(run, call, errors, exc)
        return compensated, errors

    def _begin_rollback(self, run: SagaRun) -> tuple[list[str], dict[str, str]]:
        if self.journal is not None:
            self.journal.begin_compensation(run.saga_id)
        run.transition_saga_status("compensating")
        return [], {}

    def _begin_step_rollback(
        self, run: SagaRun, name: str, errors: dict[str, str]
    ) -> tuple[Callable[..., Any] | None, ToolCall | None]:
        """Move a step into ``compensating``; undo is ``None`` when it has none."""
        step = self._step_named(name)
        if step is None or step.compensate is None:
            errors.setdefault(name, "no compensation registered")
            return None, None
        call = run.step_by_name(name)
        run.transition_step(call.step_id, StepStatus.COMPENSATING)
        if self.journal is not None:
            self.journal.begin_step_compensation(run.saga_id, call.step_id)
        return step.compensate, call

    def _end_step_rollback(
        self, run: SagaRun, call: ToolCall, result: Any, compensated: list[str]
    ) -> None:
        """The undo finished: record it as compensated."""
        run.transition_step(call.step_id, StepStatus.COMPENSATED, result=result)
        if self.journal is not None:
            self.journal.record_compensation(run.saga_id, call.step_id, result)
        compensated.append(call.name)

    def _end_step_rollback_failed(
        self, run: SagaRun, call: ToolCall, errors: dict[str, str], exc: BaseException
    ) -> None:
        """The undo raised, so the step is stuck mid-compensation: its side effect
        is still applied and needs manual attention."""
        message = f"{type(exc).__name__}: {exc}"
        errors[call.name] = message
        if self.journal is not None:
            self.journal.mark_step_compensation_failed(run.saga_id, call.step_id, message)

    @staticmethod
    async def _resolve(outcome: Any) -> Any:
        """Await a coroutine the undo returned; pass anything else through."""
        return await outcome if inspect.isawaitable(outcome) else outcome

    def _call_compensation(
        self,
        undo: Callable[..., Any],
        name: str,
        result: Any,
        context: dict[str, Any],
    ) -> Any:
        outcome = self._invoke_undo(undo, name, result, context)
        if inspect.isawaitable(outcome):
            # An `async def` undo has NOT run. Close the coroutine so it does not
            # warn later, and report it as an error rather than as compensated.
            if isinstance(outcome, Coroutine):
                outcome.close()
            raise NonRetryableError(
                f"compensation for step {name!r} is a coroutine function and cannot "
                f"run under execute(); use aexecute() or a synchronous undo"
            )
        return outcome

    async def _acall_compensation(
        self,
        undo: Callable[..., Any],
        name: str,
        result: Any,
        context: dict[str, Any],
    ) -> Any:
        return await self._resolve(self._invoke_undo(undo, name, result, context))

    def _invoke_undo(
        self,
        undo: Callable[..., Any],
        name: str,
        result: Any,
        context: dict[str, Any],
    ) -> Any:
        if self._accepts_kwargs(undo):
            return undo(step=name, result=result, **context)
        return undo()

    @staticmethod
    def _accepts_kwargs(func: Callable[..., Any] | None) -> bool:
        if func is None:
            return False
        try:
            signature = inspect.signature(func)
        except (TypeError, ValueError):  # pragma: no cover - builtins
            return False
        for param in signature.parameters.values():
            if param.kind is inspect.Parameter.VAR_KEYWORD:
                return True
        return set(signature.parameters) >= {"step", "result"}

    def _step_named(self, name: str) -> SagaStep | None:
        for step in self.steps:
            if step.name == name:
                return step
        return None


__all__ = ["Saga", "SagaStep"]

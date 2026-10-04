"""Saga runtime: sequential execution, compensation, idempotent resume, LLM-native errors.

Addresses issue #1 (saga runtime + LLM-native errors) and issue #2 (resume).
"""

import pytest

from tool_call_retry.errors import NonRetryableError, RetryExhausted, SagaFailed
from tool_call_retry.models import StepStatus
from tool_call_retry.saga import Saga, SagaStep


class Recorder:
    """Ledger of side effects so tests can assert on real compensations."""

    def __init__(self):
        self.events = []

    def log(self, *event):
        self.events.append(".".join(event))


def test_tool_decorator_registers_step_with_name_and_args():
    saga = Saga(name="checkout")

    @saga.tool("charge", amount=10)
    def charge(amount):
        return {"charge_id": "ch_1"}

    assert isinstance(saga.steps[0], SagaStep)
    assert saga.steps[0].name == "charge"
    assert saga.steps[0].tool_args == {"amount": 10}


def test_three_successful_steps_run_in_order_without_compensation(journal):
    ledger = Recorder()
    saga = Saga(name="checkout", journal=journal, saga_id="saga-ok")

    @saga.tool("charge")
    def charge():
        ledger.log("charge")
        return "ch_1"

    @saga.tool("reserve")
    def reserve():
        ledger.log("reserve")
        return "r_1"

    @saga.tool("ship")
    def ship():
        ledger.log("ship")
        return "s_1"

    run = saga.execute()
    assert run.status == "completed"
    assert ledger.events == ["charge", "reserve", "ship"]
    assert run.completed_steps == ["charge", "reserve", "ship"]
    assert journal.load_saga("saga-ok").status == "completed"


def test_step_two_failure_compensates_step_one_in_reverse(journal):
    ledger = Recorder()
    saga = Saga(name="checkout", journal=journal, saga_id="saga-fail")

    @saga.tool("charge", compensate=lambda: ledger.log("refund"))
    def charge():
        ledger.log("charge")
        return "ch_1"

    @saga.tool("ship", compensate=lambda: ledger.log("unship"))
    def ship():
        ledger.log("ship")
        raise NonRetryableError("out of stock")

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()

    assert ledger.events == ["charge", "ship", "refund"]
    assert excinfo.value.failed_step == "ship"
    assert excinfo.value.compensated == ["charge"]
    run = journal.load_saga("saga-fail")
    assert run.status == "failed"
    assert run.steps[0].status is StepStatus.COMPENSATED
    assert run.steps[1].status is StepStatus.FAILED


def test_compensation_runs_in_reverse_completion_order(journal):
    ledger = Recorder()
    saga = Saga(name="checkout", journal=journal, saga_id="saga-rev")

    @saga.tool("one", compensate=lambda: ledger.log("undo-one"))
    def one():
        ledger.log("one")
        return 1

    @saga.tool("two", compensate=lambda: ledger.log("undo-two"))
    def two():
        ledger.log("two")
        return 2

    @saga.tool("three", compensate=lambda: ledger.log("undo-three"))
    def three():
        ledger.log("three")
        return 3

    @saga.tool("four")
    def four():
        raise NonRetryableError("boom")

    with pytest.raises(SagaFailed):
        saga.execute()
    assert ledger.events[-3:] == ["undo-three", "undo-two", "undo-one"]


def test_only_completed_steps_are_compensated(journal):
    """Pending steps must never be compensated (issue #2: partial rollback)."""
    ledger = Recorder()
    saga = Saga(name="checkout", journal=journal, saga_id="saga-partial")

    @saga.tool("charge", compensate=lambda: ledger.log("refund"))
    def charge():
        ledger.log("charge")
        return "ch_1"

    @saga.tool("ship", compensate=lambda: ledger.log("unship"))
    def ship():
        ledger.log("ship")
        return "s_1"

    @saga.tool("notify", compensate=lambda: ledger.log("unnotify"))
    def notify():
        ledger.log("notify")
        raise NonRetryableError("smtp down")

    with pytest.raises(SagaFailed):
        saga.execute()
    assert ledger.events == ["charge", "ship", "notify", "unship", "refund"]
    assert "unnotify" not in ledger.events, "the failed step must not be compensated"


def test_step_without_compensation_is_recorded_as_uncompensated(journal):
    saga = Saga(name="checkout", journal=journal, saga_id="saga-nocomp")

    @saga.tool("charge")
    def charge():
        return "ch_1"

    @saga.tool("ship")
    def ship():
        raise NonRetryableError("out of stock")

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()
    assert excinfo.value.compensated == []
    assert excinfo.value.completed == ["charge"]
    assert "manual cleanup" in excinfo.value.summary()


def test_resume_skips_completed_steps_after_a_crash(journal):
    """Idempotency: a saga interrupted mid-flight does not redo finished steps."""
    calls = []

    # First process: step 1 completes, then the process dies before step 2 starts.
    first = Saga(name="checkout", journal=journal, idempotency_key="order-9")

    @first.tool("charge")
    def charge():
        calls.append("charge")
        return "ch_1"

    @first.tool("ship")
    def ship():
        calls.append("ship")
        return "s_1"

    saga_id = journal.begin_saga(idempotency_key="order-9", name="checkout")
    journal.record_step(saga_id, 1, "charge", {})
    journal.mark_step_running(saga_id, 1)
    journal.mark_step_completed(saga_id, 1, {"charge_id": "ch_1"})
    journal.record_step(saga_id, 2, "ship", {})
    del first

    # Second process: same idempotency key, the completed step must not re-run.
    resumed = Saga(name="checkout", journal=journal, idempotency_key="order-9")

    @resumed.tool("charge")
    def charge_again():
        calls.append("charge")
        return "ch_1"

    @resumed.tool("ship")
    def ship_again():
        calls.append("ship")
        return "s_1"

    run = resumed.execute()
    assert run.status == "completed"
    assert calls == ["ship"], "the completed charge must not run twice"
    assert resumed.saga_id == saga_id


def test_compensated_saga_retry_reruns_undone_steps(journal):
    """A compensated step's effect was rolled back, so a retry must redo it."""
    calls = []
    saga = Saga(name="checkout", journal=journal, idempotency_key="order-9")

    @saga.tool("charge", compensate=lambda: calls.append("refund"))
    def charge():
        calls.append("charge")
        return "ch_1"

    @saga.tool("ship")
    def ship():
        raise NonRetryableError("out of stock")

    with pytest.raises(SagaFailed):
        saga.execute()
    assert calls == ["charge", "refund"]

    retried = Saga(name="checkout", journal=journal, idempotency_key="order-9")

    @retried.tool("charge", compensate=lambda: calls.append("refund"))
    def charge2():
        calls.append("charge")
        return "ch_1"

    @retried.tool("ship")
    def ship2():
        calls.append("ship")
        return "s_1"

    run = retried.execute()
    assert run.status == "completed"
    assert calls == ["charge", "refund", "charge", "ship"]


def test_resumed_saga_adopts_the_journal_saga_id(journal):
    saga_id = journal.begin_saga(idempotency_key="order-9", name="checkout")
    journal.record_step(saga_id, 1, "charge", {})
    journal.mark_step_running(saga_id, 1)
    journal.mark_step_completed(saga_id, 1, {"charge_id": "ch_1"})

    resumed = Saga(name="checkout", journal=journal, idempotency_key="order-9")

    @resumed.tool("charge")
    def charge():
        raise AssertionError("already completed step must not re-run")

    @resumed.tool("ship")
    def ship():
        return "s_1"

    run = resumed.execute()
    assert run.saga_id == saga_id
    assert run.steps[0].attempts == 0, "an already-completed step is not re-run"
    assert run.status == "completed"


def test_transient_failures_are_retried_before_compensation(journal):
    calls = []
    saga = Saga(
        name="checkout",
        journal=journal,
        saga_id="saga-transient",
        policy_kwargs={"max_attempts": 3, "base_delay": 0.0, "jitter": "none"},
    )

    @saga.tool("charge")
    def charge():
        calls.append(1)
        if len(calls) < 3:
            raise TimeoutError("slow upstream")
        return "ch_1"

    run = saga.execute()
    assert run.status == "completed"
    assert len(calls) == 3
    assert journal.load_saga("saga-transient").steps[0].attempts == 3


def test_retry_exhaustion_triggers_compensation(journal):
    ledger = Recorder()
    saga = Saga(
        name="checkout",
        journal=journal,
        saga_id="saga-exhaust",
        policy_kwargs={"max_attempts": 2, "base_delay": 0.0, "jitter": "none"},
    )

    @saga.tool("charge", compensate=lambda: ledger.log("refund"))
    def charge():
        ledger.log("charge")
        return "ch_1"

    @saga.tool("ship", compensate=lambda: ledger.log("unship"))
    def ship():
        ledger.log("ship")
        raise TimeoutError("gateway down")

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()
    assert isinstance(excinfo.value.error, RetryExhausted)
    # 'ship' is attempted twice (max_attempts=2) and never completes, so only
    # 'charge' is rolled back.
    assert ledger.events == ["charge", "ship", "ship", "refund"]
    assert excinfo.value.compensated == ["charge"]
    assert journal.load_saga("saga-exhaust").steps[1].attempts == 2


def test_client_errors_are_not_retried(journal):
    calls = []
    saga = Saga(
        name="checkout",
        journal=journal,
        saga_id="saga-4xx",
        policy_kwargs={"max_attempts": 5, "base_delay": 0.0, "jitter": "none"},
    )

    @saga.tool("ship")
    def ship():
        calls.append(1)
        raise NonRetryableError("bad request")

    with pytest.raises(SagaFailed):
        saga.execute()
    assert len(calls) == 1


def test_llm_native_error_describes_outcome_without_stack_traces(journal):
    saga = Saga(name="checkout", journal=journal, saga_id="saga-llm")

    @saga.tool("charge", compensate=lambda: None)
    def charge():
        return "ch_1"

    @saga.tool("reserve", compensate=lambda: None)
    def reserve():
        return "r_1"

    @saga.tool("ship")
    def ship():
        raise TimeoutError("upstream timeout after 30s")

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()
    summary = excinfo.value.summary()
    assert "charge, reserve succeeded" in summary
    assert "step ship failed (TimeoutError)" in summary
    assert "reserve, charge compensated" in summary
    assert "System state is consistent." in summary
    assert "Traceback" not in summary
    assert "tool_call_retry/" not in summary


def test_saga_failed_carries_structured_context(journal):
    saga = Saga(name="checkout", journal=journal, saga_id="saga-ctx")

    @saga.tool("charge", compensate=lambda: None)
    def charge():
        return "ch_1"

    @saga.tool("ship")
    def ship():
        raise NonRetryableError("nope")

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()
    err = excinfo.value
    assert err.saga_id == "saga-ctx"
    assert err.failed_step == "ship"
    assert err.completed == ["charge"]
    assert err.compensated == ["charge"]
    assert isinstance(err.error, NonRetryableError)


def test_failing_compensation_is_reported_but_does_not_hide_the_root_cause(journal):
    ledger = Recorder()
    saga = Saga(name="checkout", journal=journal, saga_id="saga-compfail")

    @saga.tool("charge", compensate=lambda: (_ for _ in ()).throw(RuntimeError("refund API down")))
    def charge():
        ledger.log("charge")
        return "ch_1"

    @saga.tool("ship")
    def ship():
        raise NonRetryableError("out of stock")

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()
    assert excinfo.value.failed_step == "ship"
    assert "refund API down" in excinfo.value.compensation_errors["charge"]
    assert "manual cleanup" in excinfo.value.summary()
    # The undo raised, so the step is left mid-compensation, not marked done.
    assert journal.load_saga("saga-compfail").steps[0].status is StepStatus.COMPENSATING


def test_step_without_compensation_registered_does_not_break_compensation_chain(journal):
    ledger = Recorder()
    saga = Saga(name="checkout", journal=journal, saga_id="saga-mixed")

    @saga.tool("charge", compensate=lambda: ledger.log("refund"))
    def charge():
        ledger.log("charge")
        return "ch_1"

    @saga.tool("notify")  # no compensation at all
    def notify():
        ledger.log("notify")
        return "sent"

    @saga.tool("ship")
    def ship():
        raise NonRetryableError("boom")

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()
    assert excinfo.value.compensated == ["charge"]
    assert ledger.events[-1] == "refund"


def test_async_saga_awaits_async_steps_and_compensates(journal):
    """Async steps *and* async compensations: both must be awaited.

    Issue #17: this test used a sync ``lambda`` undo, so it passed even though an
    ``async def`` compensation was never awaited.
    """
    import asyncio

    ledger = Recorder()

    async def refund(**kwargs):
        await asyncio.sleep(0)
        ledger.log("refund")

    async def unship(**kwargs):
        await asyncio.sleep(0)
        ledger.log("unship")

    async def main():
        saga = Saga(name="checkout", journal=journal, saga_id="saga-async")

        @saga.tool("charge", compensate=refund)
        async def charge():
            ledger.log("charge")
            return "ch_1"

        @saga.tool("ship", compensate=unship)
        async def ship():
            ledger.log("ship")
            raise NonRetryableError("out of stock")

        return await saga.aexecute()

    with pytest.raises(SagaFailed):
        asyncio.run(main())
    # 'ship' failed, so only 'charge' is compensated -- and the async undo really ran.
    assert ledger.events == ["charge", "ship", "refund"]
    assert journal.load_saga("saga-async").steps[0].status is StepStatus.COMPENSATED


def test_step_results_are_available_to_compensation(journal):
    seen = {}

    async def main():
        saga = Saga(name="checkout", journal=journal, saga_id="saga-result")

        def compensate(**kwargs):
            seen.update(kwargs)

        @saga.tool("charge", compensate=compensate)
        def charge():
            return {"charge_id": "ch_1"}

        @saga.tool("ship")
        def ship():
            raise NonRetryableError("boom")

        await saga.aexecute()

    import asyncio

    with pytest.raises(SagaFailed):
        asyncio.run(main())
    assert seen["result"] == {"charge_id": "ch_1"}
    assert seen["step"] == "charge"


def test_execute_with_no_journal_runs_in_memory():
    saga = Saga(name="memory-only", saga_id="saga-mem")
    events = []

    @saga.tool("charge")
    def charge():
        events.append("charge")
        return "ok"

    run = saga.execute()
    assert run.status == "completed"
    assert run.saga_id == "saga-mem"
    assert events == ["charge"]


def test_duplicate_step_names_are_rejected():
    saga = Saga(name="checkout", saga_id="saga-dup")

    @saga.tool("charge")
    def charge():
        return "ok"

    with pytest.raises(ValueError, match="duplicate step name"):

        @saga.tool("charge")
        def charge_again():
            return "ok"


def test_executing_a_saga_with_no_steps_fails_fast():
    with pytest.raises(ValueError, match="no steps"):
        Saga(name="empty", saga_id="saga-empty").execute()


def test_journal_is_required_for_idempotent_resume(journal):
    saga = Saga(name="checkout", saga_id="saga-nojournal", idempotency_key="order-9")
    saga.add_step("charge", lambda: "ok")
    with pytest.raises(ValueError, match="journal"):
        saga.execute()

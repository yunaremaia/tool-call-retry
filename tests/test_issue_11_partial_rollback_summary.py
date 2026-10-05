"""A partially failed rollback must not be reported as a consistent system.

Issue #11: ``SagaFailed.summary()`` branched on ``self.compensated`` alone. When
*some* undos succeeded and *another* raised, ``compensated`` was non-empty, so
the first branch won and the summary claimed "System state is consistent" --
while the step whose undo raised stayed in ``COMPENSATING`` with its side effect
still applied. The README promises the opposite: "the summary then says the
system may be inconsistent and needs manual cleanup".

The pre-existing tests only covered the all-fail case, where ``compensated`` is
empty and the third branch happened to be correct.
"""

import pytest

from tool_call_retry.errors import NonRetryableError, SagaFailed
from tool_call_retry.models import StepStatus
from tool_call_retry.saga import Saga


def _boom(message):
    def do():
        raise NonRetryableError(message)

    return do


def test_mixed_rollback_outcome_is_not_reported_as_consistent(journal):
    """Two undos succeed, one raises: the system is NOT consistent."""
    saga = Saga(name="checkout", journal=journal, saga_id="saga-mixed-rollback")

    @saga.tool("charge", compensate=lambda: None)
    def charge():
        return "ch_1"

    @saga.tool("reserve", compensate=lambda: (_ for _ in ()).throw(RuntimeError("refund down")))
    def reserve():
        return "rs_1"

    @saga.tool("audit", compensate=lambda: None)
    def audit():
        return "ok"

    saga.add_step("ship", _boom("out of stock"))

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()

    err = excinfo.value
    # Precondition for the bug: the failure was *partial*, not total.
    assert err.compensated, "this test needs at least one undo to have succeeded"
    assert err.compensation_errors, "this test needs at least one undo to have raised"

    summary = err.summary()
    assert "System state is consistent" not in summary, (
        f"a partially failed rollback was reported as consistent: {summary!r}"
    )
    assert "may be inconsistent" in summary
    assert "manual cleanup" in summary
    # The stuck step is named, and so is what did unwind.
    assert "reserve" in summary
    assert "audit, charge compensated" in summary


def test_partial_rollback_leaves_the_failed_undo_step_stuck(journal):
    """The undo that raised must stay mid-compensation, not be marked done."""
    saga = Saga(name="checkout", journal=journal, saga_id="saga-mixed-stuck")

    @saga.tool("charge", compensate=lambda: None)
    def charge():
        return "ch_1"

    @saga.tool("reserve", compensate=lambda: (_ for _ in ()).throw(RuntimeError("refund down")))
    def reserve():
        return "rs_1"

    saga.add_step("ship", _boom("boom"))

    with pytest.raises(SagaFailed):
        saga.execute()

    steps = {s.name: s for s in journal.load_saga("saga-mixed-stuck").steps}
    assert steps["charge"].status is StepStatus.COMPENSATED
    assert steps["reserve"].status is StepStatus.COMPENSATING, (
        "an undo that raised must not be recorded as compensated"
    )


def test_clean_rollback_still_reports_consistent(journal):
    """Guard the other side of the fix: a fully clean rollback is consistent."""
    saga = Saga(name="checkout", journal=journal, saga_id="saga-clean")

    @saga.tool("charge", compensate=lambda: None)
    def charge():
        return "ch_1"

    saga.add_step("ship", _boom("boom"))

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()

    assert excinfo.value.compensated == ["charge"]
    assert excinfo.value.compensation_errors == {}
    assert "System state is consistent." in excinfo.value.summary()

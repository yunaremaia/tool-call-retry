"""Async ``compensate`` callables must run before the saga reports them compensated.

Issue #17: ``_fail()`` reached the sync ``_call_compensation()`` from *both*
execution paths, so an ``async def`` undo produced a coroutine that was never
awaited. The step was still journalled as ``compensated`` and
``SagaFailed.summary()`` claimed "System state is consistent" while the side
effect stayed applied.
"""

import asyncio

import pytest

from tool_call_retry.errors import NonRetryableError, SagaFailed
from tool_call_retry.models import StepStatus
from tool_call_retry.saga import Saga

pytestmark = pytest.mark.filterwarnings("error::RuntimeWarning")


def _boom(message):
    def do():
        raise NonRetryableError(message)

    return do


def test_aexecute_awaits_an_async_compensation_and_reports_it(journal):
    ledger = []

    async def refund(step=None, result=None):
        await asyncio.sleep(0)
        ledger.append(f"refund:{step}:{result['charge_id']}")
        return "refunded"

    saga = Saga(name="checkout", journal=journal, saga_id="saga-async-comp")

    @saga.tool("charge", compensate=refund)
    async def charge():
        ledger.append("charge")
        return {"charge_id": "ch_1"}

    saga.add_step("ship", _boom("out of stock"))

    with pytest.raises(SagaFailed) as excinfo:
        asyncio.run(saga.aexecute())

    # The undo really ran, and the saga only reported it afterwards.
    assert ledger == ["charge", "refund:charge:ch_1"]
    assert excinfo.value.compensated == ["charge"]
    assert excinfo.value.compensation_errors == {}
    assert journal.load_saga("saga-async-comp").steps[0].status is StepStatus.COMPENSATED


def test_aexecute_awaits_an_async_compensation_mixed_with_sync_steps(journal):
    """The README advertises mixing sync and async steps in one saga."""
    ledger = []

    async def refund(**kwargs):
        await asyncio.sleep(0)
        ledger.append("refund")
        return "refunded"

    saga = Saga(name="checkout", journal=journal, saga_id="saga-mixed")
    saga.add_step(
        "charge", lambda: ledger.append("charge") or "ch_1", compensate=refund
    )
    saga.add_step("ship", _boom("boom"))

    with pytest.raises(SagaFailed) as excinfo:
        asyncio.run(saga.aexecute())

    assert ledger == ["charge", "refund"]
    assert excinfo.value.compensated == ["charge"]


def test_async_compensation_failure_is_reported_not_swallowed(journal):
    """Awaiting the undo must not turn its failure into a silent success."""
    ledger = []

    async def refund(**kwargs):
        await asyncio.sleep(0)
        raise RuntimeError("refund API down")

    saga = Saga(name="checkout", journal=journal, saga_id="saga-async-comp-fail")
    saga.add_step("charge", lambda: ledger.append("charge") or "ch_1", compensate=refund)
    saga.add_step("ship", _boom("boom"))

    with pytest.raises(SagaFailed) as excinfo:
        asyncio.run(saga.aexecute())

    assert excinfo.value.compensated == []
    assert "refund API down" in excinfo.value.compensation_errors["charge"]
    assert "System state is consistent" not in excinfo.value.summary()


def test_sync_execute_never_reports_a_coroutine_undo_as_compensated(journal):
    """``execute()`` cannot await, so it must report the coroutine as an error."""
    ledger = []

    async def refund(**kwargs):
        ledger.append("refund")

    saga = Saga(name="checkout", journal=journal, saga_id="saga-sync-coroutine")
    saga.add_step("charge", lambda: ledger.append("charge") or "ch_1", compensate=refund)
    saga.add_step("ship", _boom("out of stock"))

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()

    assert ledger == ["charge"], "execute() cannot run the coroutine; it must not lie"
    assert excinfo.value.compensated == []
    assert "charge" in excinfo.value.compensation_errors
    assert "System state is consistent" not in excinfo.value.summary()
    assert journal.load_saga("saga-sync-coroutine").steps[0].status is (
        StepStatus.COMPENSATING
    )

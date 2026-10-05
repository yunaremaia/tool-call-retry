"""A step result the journal refuses must not orphan the saga as active.

Issue #30: ``Saga.execute()``/``aexecute()`` recorded a step's result *outside*
the ``try`` that guards the step. When ``_dumps`` refused a non-JSON-native
result the ``TypeError`` escaped before ``_fail()`` ran, so nothing compensated,
no ``SagaFailed`` was raised, and the saga stayed ``active`` forever -- an
unreclaimable orphan that every later resume rejects with ``InvalidTransition``.

The refusal itself is correct (#23/#29); what was missing is a compensation path
around the *succeed* side, the mirror of the one already hardened on the *undo*
side (``test_an_unpersistable_undo_result_does_not_mask_the_saga_failure``).
"""

import asyncio
import decimal

import pytest

from tool_call_retry.errors import SagaFailed
from tool_call_retry.saga import Saga

SAGA_ID = "saga-issue-30"


def _checkout(journal):
    """A saga whose second step returns a value the journal refuses to store."""
    saga = Saga(name="checkout", journal=journal, saga_id=SAGA_ID)
    saga.add_step(
        "charge_card",
        lambda: {"charge_id": "ch_1"},
        compensate=lambda **kwargs: {"refund_id": "rf_1"},
    )
    saga.add_step("reserve_inventory", lambda: decimal.Decimal("1.50"))
    return saga


def _saga_status(journal):
    return journal.list_sagas()[0]["status"]


def test_an_unpersistable_result_compensates_instead_of_escaping_as_a_typeerror(journal):
    """No raw ``TypeError`` may reach the caller: the caller gets ``SagaFailed``."""
    saga = _checkout(journal)

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()

    failure = excinfo.value
    assert failure.failed_step == "reserve_inventory"
    assert isinstance(failure.root_cause(), TypeError)
    # The step before it really was rolled back.
    assert failure.compensated == ["charge_card"]


def test_the_saga_is_left_terminal_rather_than_orphaned_as_active(journal):
    """The core of the damage: an ``active`` saga is never reclaimed by cleanup."""
    saga = _checkout(journal)

    with pytest.raises(SagaFailed):
        saga.execute()

    assert _saga_status(journal) == "failed"
    assert journal.recover_pending() == []


def test_the_saga_summary_names_the_compensation(journal):
    """The README's one-sentence LLM-facing report is what the caller consumes."""
    saga = _checkout(journal)

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()

    summary = excinfo.value.summary()
    assert "reserve_inventory" in summary
    assert "charge_card compensated" in summary
    assert "consistent" in summary


def test_the_async_path_compensates_too(journal):
    """``aexecute`` had the identical shape and the identical defect."""
    saga = _checkout(journal)

    with pytest.raises(SagaFailed) as excinfo:
        asyncio.run(saga.aexecute())

    assert excinfo.value.compensated == ["charge_card"]
    assert _saga_status(journal) == "failed"


def test_a_storable_result_still_behaves_exactly_as_before(journal):
    """No regression: the ordinary success path is untouched."""
    saga = Saga(name="checkout", journal=journal, saga_id=SAGA_ID)
    saga.add_step("charge_card", lambda: {"charge_id": "ch_1"})
    saga.add_step("ship_package", lambda: {"tracking": "t_1"})

    run = saga.execute()

    assert run.status == "completed"
    assert _saga_status(journal) == "completed"
    assert [step.result for step in journal.load_saga(SAGA_ID).steps] == [
        {"charge_id": "ch_1"},
        {"tracking": "t_1"},
    ]


def test_memory_and_the_journal_agree_on_the_step_states(journal):
    """The step must never be recorded ``completed`` when nothing was persisted.

    ``_succeed`` used to transition in memory *before* the journal write, so a
    refused write left the two disagreeing about a step stored nowhere at all.
    """
    saga = _checkout(journal)

    with pytest.raises(SagaFailed):
        saga.execute()

    steps = {step.name: step for step in journal.load_saga(SAGA_ID).steps}
    assert steps["charge_card"].status == "compensated"
    assert steps["reserve_inventory"].status == "failed"
    # Nothing unpersistable was written, so recovery has no dangling result.
    assert steps["reserve_inventory"].result is None

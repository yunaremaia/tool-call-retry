"""A step result that cannot be persisted must fail loudly, not degrade silently.

Issue #23: ``SagaJournal`` stored every value with ``json.dumps(value, default=str)``,
so a ``Decimal`` became ``'1.50'`` and a ``datetime`` became its ``str()``. After a
crash-and-resume the compensation received **strings** while an in-process run
received the live objects, with nothing to distinguish the two.
"""

import datetime
import decimal
import json

import pytest

from tool_call_retry.errors import NonRetryableError, SagaFailed
from tool_call_retry.journal import _dumps, _loads
from tool_call_retry.saga import Saga


def test_dumps_refuses_to_silently_stringify_an_unencodable_value():
    with pytest.raises(TypeError) as excinfo:
        _dumps({"amount": decimal.Decimal("1.50")})

    message = str(excinfo.value)
    assert "Decimal" in message, "the error must name the type that cannot be stored"
    assert "JSON" in message or "serial" in message.lower()


def test_dumps_still_passes_through_json_native_values():
    assert json.loads(_dumps({"amount": 1.5, "ids": [1, 2], "ok": True})) == {
        "amount": 1.5,
        "ids": [1, 2],
        "ok": True,
    }
    assert _dumps(None) is None


def test_a_result_that_cannot_be_persisted_is_rejected_at_the_write_site(journal):
    """The write must fail rather than store a value recovery cannot reproduce."""
    saga_id = journal.begin_saga(None, name="checkout")
    journal.record_step(saga_id, 1, "charge_card", {})
    journal.mark_step_running(saga_id, 1)

    with pytest.raises(TypeError):
        journal.mark_step_completed(
            saga_id, 1, {"amount": decimal.Decimal("1.50"), "at": datetime.datetime(2024, 1, 1)}
        )


def test_non_json_tool_args_are_rejected_rather_than_stringified(journal):
    """Same hole as the result: args are passed back into ``step.do`` on resume."""
    saga_id = journal.begin_saga(None, name="checkout")
    with pytest.raises(TypeError):
        journal.record_step(saga_id, 1, "charge_card", {"amount": decimal.Decimal("1.50")})


def test_reloaded_result_matches_the_original_for_json_native_values(journal):
    """The recovery contract: ``result=<the step's result>`` must actually hold."""
    result = {"amount": 1.5, "at": "2024-01-01", "ids": [1, 2, 3]}
    saga_id = journal.begin_saga(None, name="checkout")
    journal.record_step(saga_id, 1, "charge_card", {"amount": 1.5})
    journal.mark_step_running(saga_id, 1)
    journal.mark_step_completed(saga_id, 1, result)

    assert journal.load_saga(saga_id).steps[0].result == result


def test_loads_does_not_turn_a_corrupt_row_into_a_bare_string():
    """A decode failure used to flow a raw ``str`` into a compensation result."""
    with pytest.raises(ValueError):
        _loads("{not valid json")


def test_loads_passes_through_a_valid_document(journal):
    assert _loads('{"a": 1}') == {"a": 1}
    assert _loads(None) is None


def test_an_unpersistable_undo_result_does_not_mask_the_saga_failure(journal):
    """Rejecting a lossy write must not hide the real error or abort the undos.

    A journal failure is an observer, exactly like the ``on_attempt``/sleeper
    callbacks fixed in #25: it is reported, but the root cause still surfaces.
    """
    saga = Saga(name="checkout", journal=journal, saga_id="saga-unpersistable-undo")

    def charge():
        return "ch_1"

    def refund_one(**kwargs):
        # Cannot be journalled -- the undo itself ran fine.
        return decimal.Decimal("1.50")

    saga.add_step("one", charge, compensate=refund_one)
    saga.add_step("two", charge, compensate=lambda **kwargs: "undone-two")

    def boom():
        raise NonRetryableError("out of stock")

    saga.add_step("boom", boom)

    with pytest.raises(SagaFailed) as excinfo:
        saga.execute()

    # The real failure is still reported...
    assert excinfo.value.failed_step == "boom"
    assert isinstance(excinfo.value.root_cause(), NonRetryableError)
    # ...the other undo still ran, rather than being skipped by the write failure...
    assert excinfo.value.compensated == ["two"]
    # ...and the undo that could not be journalled is reported as an error.
    assert "not JSON-serialisable" in excinfo.value.compensation_errors["one"]

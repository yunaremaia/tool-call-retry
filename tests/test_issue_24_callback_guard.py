"""Regression tests for issue #24 / PR #25.

Every test here FAILS on main (65987fc) and PASSES on the PR head, because
main lets an exception from `on_attempt` escape `run()`/`arun()` and replace
the real failure with no __cause__.
"""

from __future__ import annotations

import asyncio

import pytest

from tool_call_retry.errors import RetryExhausted
from tool_call_retry.models import RetryAttempt
from tool_call_retry.policy import RetryPolicy


class Terminal(Exception):
    """Not in retryable_exceptions -> terminal."""


class Transient(Exception):
    """In retryable_exceptions -> retried."""


def make_policy(**overrides):
    kwargs = dict(
        max_attempts=3,
        base_delay=5.0,
        multiplier=1.0,
        max_total_delay=100.0,
        jitter="none",
        retryable_exceptions=(Transient,),
    )
    kwargs.update(overrides)
    return RetryPolicy(**kwargs)


def exploding_observer(record):
    raise ValueError("observer exploded")


# --- the bug: observer failure must not replace the real error ------------


def test_terminal_error_survives_exploding_observer():
    policy = make_policy()

    def func():
        raise Terminal("the real error")

    with pytest.raises(Terminal, match="the real error"):
        policy.run(func, on_attempt=exploding_observer)


def test_retryable_error_survives_exploding_observer():
    policy = make_policy()

    def func():
        raise Transient("the real transient")

    with pytest.raises(RetryExhausted) as excinfo:
        policy.run(func, sleep=lambda d: None, on_attempt=exploding_observer)

    assert isinstance(excinfo.value.last_error, Transient)
    assert "the real transient" in str(excinfo.value)


def test_cause_is_the_real_error_not_the_observer():
    """Issue #24: the __cause__ must be the wrapped error, not the observer's."""
    policy = make_policy()

    def func():
        raise Transient("the real transient")

    with pytest.raises(RetryExhausted) as excinfo:
        policy.run(func, sleep=lambda d: None, on_attempt=exploding_observer)

    cause = excinfo.value.__cause__
    assert isinstance(cause, Transient), f"__cause__ was {cause!r}"
    assert "observer exploded" not in str(cause)


def test_success_is_not_turned_into_failure():
    policy = make_policy()
    assert policy.run(lambda: "fine", on_attempt=exploding_observer) == "fine"


def test_base_exception_from_observer_still_propagates():
    """The guard catches Exception, not BaseException: Ctrl-C must survive."""
    policy = make_policy()

    def observer(record):
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        policy.run(lambda: "fine", on_attempt=observer)


# --- async twin ----------------------------------------------------------


def test_async_terminal_error_survives_exploding_observer():
    policy = make_policy()

    async def func():
        raise Terminal("the real async error")

    with pytest.raises(Terminal, match="the real async error"):
        asyncio.run(policy.arun(func, on_attempt=exploding_observer))


def test_async_success_is_not_turned_into_failure():
    policy = make_policy()

    async def func():
        return "afine"

    assert asyncio.run(policy.arun(func, on_attempt=exploding_observer)) == "afine"


# --- the sleeper guard ---------------------------------------------------


def test_sleeper_failure_does_not_escape_and_is_recorded():
    """A broken sleeper must not replace the real retryable error."""
    policy = make_policy()

    def bad_sleeper(delay):
        raise OSError("clock exploded")

    def func():
        raise Transient("transient boom")

    with pytest.raises(RetryExhausted) as excinfo:
        policy.run(func, sleep=bad_sleeper)

    assert isinstance(excinfo.value.__cause__, Transient)
    errors = [r.error or "" for r in excinfo.value.attempt_history]
    assert any("sleep failed: OSError('clock exploded')" in e for e in errors), errors
    # the sleep-failure note must never be prefixed with a literal "None"
    assert not any(e.startswith("None ") for e in errors), errors


def test_sleeper_failure_is_bounded_by_max_attempts():
    """The swallowed sleeper failure must not create an unbounded loop."""
    policy = make_policy()
    calls = {"func": 0}

    def func():
        calls["func"] += 1
        raise Transient("boom")

    with pytest.raises(RetryExhausted):
        policy.run(func, sleep=lambda d: (_ for _ in ()).throw(OSError("nope")))

    assert calls["func"] == policy.max_attempts


# --- RetryAttempt must stay mutable (guards against a frozen dataclass) ---


def test_retry_attempt_is_mutable():
    """`record.error = ...` inside the handlers would raise if frozen."""
    record = RetryAttempt(step_id=0, attempt=1)
    record.error = "mutated"
    assert record.error == "mutated"

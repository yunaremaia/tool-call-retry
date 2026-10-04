"""Backoff window must never overflow for a large-but-legal attempt count.

Regression coverage for issue #22 (PR #26). ``backoff_window`` used to compute
``base_delay * multiplier ** (attempt - 1)`` *before* clamping to ``max_delay``,
so a legal configuration with a large attempt count or multiplier raised
``OverflowError`` instead of returning the cap.
"""

import builtins

import pytest

from tool_call_retry.policy import RetryPolicy


@pytest.mark.parametrize(
    ("base_delay", "multiplier", "max_delay", "attempt"),
    [
        (1.0, 1e3, 30.0, 1024),
        (1.0, 1e3, 30.0, 1025),
        (1.0, 1e3, 30.0, 100_000),
        (0.5, 2.0, 60.0, 100_000),
        (0.1, 2.0, 30.0, 100_000),  # library defaults
        (1.0, 1e300, 30.0, 10),
    ],
)
def test_backoff_window_is_capped_instead_of_overflowing(
    base_delay: float, multiplier: float, max_delay: float, attempt: int
) -> None:
    policy = RetryPolicy(base_delay=base_delay, multiplier=multiplier, max_delay=max_delay)
    assert policy.backoff_window(attempt) == pytest.approx(max_delay)


def test_backoff_window_of_a_constant_series_never_overflows() -> None:
    policy = RetryPolicy(base_delay=2.0, multiplier=1.0, max_delay=30.0)
    assert policy.backoff_window(100_000) == pytest.approx(2.0)


def test_backoff_window_with_zero_base_delay_stays_zero() -> None:
    policy = RetryPolicy(base_delay=0.0, multiplier=2.0, max_delay=30.0)
    assert policy.backoff_window(100_000) == 0.0


def test_delay_for_is_bounded_by_max_delay_at_huge_attempt_counts() -> None:
    policy = RetryPolicy(base_delay=1.0, multiplier=1e3, max_delay=30.0, jitter="none")
    assert policy.delay_for(100_000) == pytest.approx(30.0)


def test_backoff_window_matches_the_geometric_series_below_the_cap() -> None:
    """The cap short-circuit must not shift the series by one attempt."""
    policy = RetryPolicy(base_delay=0.25, multiplier=3.0, max_delay=25.0)
    for attempt in range(1, 12):
        expected = builtins.min(0.25 * 3.0 ** (attempt - 1), 25.0)
        assert policy.backoff_window(attempt) == pytest.approx(expected)


def test_backoff_window_still_rejects_zero_attempt() -> None:
    with pytest.raises(ValueError, match="1-based"):
        RetryPolicy().backoff_window(0)

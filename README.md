# tool-call-retry

[![CI](https://github.com/yunaremaia/tool-call-retry/actions/workflows/ci.yml/badge.svg)](https://github.com/yunaremaia/tool-call-retry/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/github/license/yunaremaia/tool-call-retry)](LICENSE)
[![Release](https://img.shields.io/github/v/release/yunaremaia/tool-call-retry)](https://github.com/yunaremaia/tool-call-retry/releases/latest)

Saga-pattern runtime for AI agent tool calls: a retry policy for transient
failures, a SQLite journal for crash recovery, and compensation for steps that
already succeeded.

## The Problem

An agent runs three tools in sequence. Step 3 fails. Steps 1 and 2 already
changed the world — a card was charged, inventory was reserved — and nothing
rolled them back. Worse, if the agent retries the whole workflow it may charge
the card twice.

Three separate gaps:

- **No backoff**: a rate-limited API (HTTP 429) is retried instantly, or not at all.
- **No rollback**: a partially-applied workflow leaves the system inconsistent.
- **No idempotency**: re-running the workflow duplicates its side effects.

## What This Does

```python
from tool_call_retry import Saga, SagaJournal, NonRetryableError

saga = Saga(name="checkout", journal=SagaJournal("saga.db"), idempotency_key="order-123")

@saga.tool("charge_card", amount=100, compensate=refund_card)
def charge_card(amount):
    return payment_client.charge(amount)

@saga.tool("ship_package", address="Rua X")
def ship_package(address):
    return shipping_client.ship(address)   # raises on out-of-stock

try:
    run = saga.execute()
    print(run.status)                      # "completed"
except Exception as exc:
    print(exc.summary())
    # steps charge_card succeeded, step ship_package failed (NonRetryableError),
    # steps charge_card compensated. System state is consistent.
```

If `ship_package` raises, `charge_card` is compensated in reverse order and the
failure is described in one sentence an agent can act on, with no traceback.

## Features

- **Retry policy** — exponential backoff with full jitter, attempt cap and total
  delay budget. Retries timeouts, connection errors, HTTP 429 and 5xx; never
  retries other 4xx client errors.
- **Compensation** — every completed step is rolled back in reverse order when a
  later step fails. Steps that never completed are left alone.
- **Crash recovery** — SQLite journal in WAL mode. A process killed mid-saga
  leaves a resumable record; the next run skips finished steps.
- **Idempotency** — an `idempotency_key` maps to one saga. Re-running the same key
  resumes the existing saga instead of duplicating side effects.
- **LLM-native errors** — `SagaFailed.summary()` reports what succeeded, what
  failed and what was rolled back, in prose.
- **CLI** — `run`, `status`, `recover`, `journal`, `list`, `cleanup`, with
  `--json` for machine-readable output.
- **Async** — `aexecute()` for coroutine steps, with the same retry and
  compensation semantics.

## Install

Not on PyPI yet. Install from source:

```bash
pip install git+https://github.com/yunaremaia/tool-call-retry.git
```

Or from a checkout, for development:

```bash
git clone https://github.com/yunaremaia/tool-call-retry.git
cd tool-call-retry
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Requires Python 3.10+. The only runtime dependency is PyYAML (for saga config
files); the journal uses stdlib `sqlite3`.

## Retry Policy

`RetryPolicy` is usable on its own, with no saga involved:

```python
from tool_call_retry import RetryPolicy, HTTPStatusError

policy = RetryPolicy(max_attempts=5, base_delay=0.2, multiplier=2.0, jitter="full")

result = policy.run(client.get, "/orders/123")
```

Defaults: `max_attempts=3`, `base_delay=0.1`, `multiplier=2.0`,
`max_delay=30.0`, `max_total_delay=60.0`, `jitter="full"`.

**Backoff.** The window for attempt *n* is `base_delay * multiplier ** (n - 1)`,
capped at `max_delay`. With `jitter="full"` the sleep is drawn uniformly from
`[0, window]`, which is the AWS "full jitter" recipe — it de-synchronises
thundering herds better than a fixed delay. `jitter="sequential"` uses
`[window/2, window]`; `jitter="none"` always sleeps the full window.

**What gets retried.**

| Error | Retried |
|---|---|
| `TimeoutError`, `ConnectionError` | yes |
| `HTTPStatusError(429)`, any 5xx | yes |
| `HTTPStatusError(400/401/403/404/422)` | no |
| `ToolCallError(retryable=False)`, `NonRetryableError` | no |
| `ValueError`, `KeyError`, `TypeError` | no |

Custom rules go through the `retryable_exceptions` field, which accepts exception
types and `(type, predicate)` pairs:

```python
import errno
from tool_call_retry import RetryPolicy

policy = RetryPolicy(
    retryable_exceptions=[(OSError, lambda e: e.errno == errno.ECONNRESET)]
)
assert policy.is_retryable(OSError(errno.ECONNRESET, "reset"))
```

Use the `errno` constants rather than hardcoded numbers: `ECONNRESET` is 104 on
Linux and 110 on macOS.

A policy can also be loaded from YAML:

```yaml
# retry.yml
max_attempts: 4
base_delay: 0.5
jitter: sequential
```

```python
policy = RetryPolicy.from_yaml("retry.yml")
```

## Journal and Crash Recovery

Every state change is committed to SQLite, so a saga interrupted by a kill or a
crash is recoverable:

```python
from tool_call_retry import SagaJournal

journal = SagaJournal("saga.db")
journal.recover_pending()      # sagas that are neither completed nor failed
journal.resume_saga(saga_id)   # step numbers still to run
journal.load_saga(saga_id)     # full step-by-step state
```

`resume_saga` resets steps stuck in `running` to `pending`: a process that died
mid-call leaves the side effect unknown, so the step is retried under its
idempotency key. Steps recorded as `completed` are skipped.

Old terminal sagas are removed with `journal.cleanup(max_age_seconds=...)`, which
also cascades to their steps and attempt records.

## Compensation Semantics

- Compensation runs **only** for steps that completed. A failed or never-started
  step is not compensated — there is nothing to undo.
- Order is **reverse** execution order, so dependencies unwind correctly.
- A compensation that raises does not stop the remaining ones. That step is left
  in `compensating` and its error is reported in
  `SagaFailed.compensation_errors`; the summary then says the system may be
  inconsistent and needs manual cleanup.
- Retrying a saga whose steps were compensated re-runs those steps: compensation
  rolled their side effects back, so they are no longer done.

## CLI

```bash
tool-call-retry run --config saga.yml          # execute a saga
tool-call-retry status --saga-id <id>         # show saga state
tool-call-retry recover [--resume --config f] # list (and resume) interrupted sagas
tool-call-retry journal --saga-id <id>        # full operation log
tool-call-retry list                           # all recorded sagas
tool-call-retry cleanup --max-age 604800       # delete old terminal sagas
```

Add `--json` to any subcommand for machine-readable output. Exit codes: `0`
success, `1` saga failure, `2` CLI misuse.

A runnable example lives in [`examples/`](examples/):

```bash
PYTHONPATH=examples tool-call-retry run --config examples/checkout.yml
```

### Saga config

Steps reference callables as `module:function`:

```yaml
name: checkout
idempotency_key: order-123
journal: saga.db

retry:
  max_attempts: 3
  base_delay: 0.1
  jitter: full

steps:
  - name: charge_card
    do: mypkg.tools:charge
    compensate: mypkg.tools:refund     # optional
    args: {amount: 100}
    idempotency_key: charge-order-123   # optional, UNIQUE in the journal
    max_attempts: 5                     # optional, overrides the saga policy
```

`compensate` is called with `step=<name>` and `result=<the step's result>` when it
accepts keyword arguments; otherwise it is called with no arguments.

## Async

```python
run = await saga.aexecute()
```

`aexecute` awaits coroutine steps and handles plain functions identically. Mixing
them in one saga works. Calling sync `execute()` with an async step raises rather
than silently doing the wrong thing.

## Development

```bash
pip install -e ".[dev]"
pytest              # 168 tests
ruff check .
```

CI runs the suite on Python 3.10, 3.11 and 3.12, plus a `ruff check` lint job.

## Not Implemented

Named honestly so nobody plans around them:

- **No LLM-based inverse derivation.** Compensation is the `compensate` callable
  you register per step, not an LLM deriving the undo from the call and its
  result. The original README advertised this; no code exists for it.
- **No parallel step execution.** Steps run sequentially.
- **No distributed lock.** Two processes resuming the same saga id concurrently
  can double-run a step; the journal is not a mutex.
- **No Redis or non-SQLite storage backend.** `SagaJournal` is SQLite only.
- **Not published on PyPI.** Install from git.
- **No CLI schema validation** beyond malformed YAML and missing callables.

If this tool is useful to you, a star helps other people find it.

## Related tools

- **[agent-guard](https://github.com/yunaremaia/agent-guard)** — enforce guardrails on AI agent tool calls
- **[mcp-guard](https://github.com/yunaremaia/mcp-guard)** — audit MCP servers for unsafe permissions
- **[context-bridge](https://github.com/yunaremaia/context-bridge)** — persistent session memory for AI agents
- **[ci-test-gate](https://github.com/yunaremaia/ci-test-gate)** — block PRs until the required tests actually run

Part of a family of focused, single-purpose developer tools — each one does one thing
and does it well.

## License

MIT

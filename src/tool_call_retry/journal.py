"""SQLite-backed saga journal for crash recovery and idempotency.

Implements the schema from issue #2 plus a small append-only operation log so
``tool-call-retry journal`` can show what actually happened. Durability comes
from WAL mode and one transaction per mutation: a process that dies mid-saga
leaves the last committed state intact and every step still in ``running`` is
resumable (its side effect is unknown, so the runtime re-runs it under the same
idempotency key).
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from tool_call_retry.errors import InvalidTransition
from tool_call_retry.models import (
    STEP_TRANSITIONS,
    RetryAttempt,
    SagaRun,
    StepStatus,
    ToolCall,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS sagas (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT UNIQUE,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS steps (
    saga_id TEXT NOT NULL,
    step_number INTEGER NOT NULL,
    tool_name TEXT NOT NULL,
    tool_args TEXT NOT NULL,
    result TEXT,
    error TEXT,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT UNIQUE,
    started_at REAL,
    finished_at REAL,
    PRIMARY KEY (saga_id, step_number),
    FOREIGN KEY (saga_id) REFERENCES sagas(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS retry_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    saga_id TEXT NOT NULL,
    step_number INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    delay REAL NOT NULL DEFAULT 0.0,
    error TEXT,
    succeeded INTEGER NOT NULL DEFAULT 0,
    ts REAL NOT NULL,
    FOREIGN KEY (saga_id) REFERENCES sagas(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    saga_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    timestamp REAL NOT NULL,
    payload TEXT
);

CREATE INDEX IF NOT EXISTS idx_steps_saga ON steps(saga_id);
CREATE INDEX IF NOT EXISTS idx_attempts_saga ON retry_attempts(saga_id, step_number);
CREATE INDEX IF NOT EXISTS idx_operations_saga ON operations(saga_id, id);
"""

#: Statuses that cannot be resumed: the saga is done, one way or the other.
TERMINAL_SAGA_STATUSES = ("completed", "failed")
RESUMABLE_STEP_STATUSES = (
    StepStatus.PENDING.value,
    StepStatus.RUNNING.value,
)


def _dumps(value: Any) -> str | None:
    """Encode for storage, refusing to silently change the value's type.

    ``json.dumps(..., default=str)`` would store ``Decimal("1.50")`` as ``'1.50'``
    and a ``datetime`` as its ``str()``. An in-process run would then hand the live
    object to ``compensate`` while a crash-resume handed it a string, so the undo
    raised (or silently rolled back the wrong thing). Better to refuse the write
    than persist a value recovery cannot reproduce (issue #23).
    """
    if value is None:
        return None
    try:
        return json.dumps(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(
            f"step data is not JSON-serialisable ({exc}); it cannot be journalled "
            f"and would not survive crash recovery. Return a JSON-native value "
            f"(dict/list/str/int/float/bool/None), or keep it out of the result."
        ) from exc


def _loads(raw: str | None) -> Any:
    if raw is None:
        return None
    # A decode failure used to fall back to the raw string, so a corrupt row turned
    # a compensation's ``result`` into a str. Surface it instead (issue #23).
    return json.loads(raw)


class SagaJournal:
    """Persistence layer for :class:`~tool_call_retry.models.SagaRun`.

    Args:
        db_path: SQLite file path, or ``":memory:"`` for an ephemeral journal.
    """

    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.db_path = str(db_path)
        if self.db_path not in (":memory:", ""):
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        if self.db_path not in (":memory:", ""):
            self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = FULL")
        self._conn.executescript(SCHEMA)

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SagaJournal:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- saga lifecycle ---------------------------------------------------
    def begin_saga(
        self,
        saga_id: str | None = None,
        *,
        idempotency_key: str | None = None,
        name: str = "",
    ) -> str:
        """Create (or re-attach to) a saga and return its id.

        When ``idempotency_key`` matches an existing saga, that saga's id is
        returned and nothing is written — this is the idempotency guarantee
        from issue #2 ("re-running with the same key returns existing saga state,
        not duplicate").
        """
        existing = self.find_saga_by_key(idempotency_key) if idempotency_key else None
        if existing is not None:
            return existing
        new_id = saga_id or f"saga-{uuid.uuid4().hex[:12]}"
        now = time.time()
        self._conn.execute(
            "INSERT INTO sagas (id, name, idempotency_key, status, created_at, updated_at)"
            " VALUES (?, ?, ?, 'active', ?, ?)",
            (new_id, name, idempotency_key, now, now),
        )
        self._log(new_id, "begin_saga", {"name": name, "idempotency_key": idempotency_key})
        return new_id

    def find_saga_by_key(self, idempotency_key: str | None) -> str | None:
        if not idempotency_key:
            return None
        row = self._conn.execute(
            "SELECT id FROM sagas WHERE idempotency_key = ?", (idempotency_key,)
        ).fetchone()
        return row["id"] if row else None

    def load_saga(self, saga_id: str) -> SagaRun:
        """Rebuild the in-memory run, or raise ``KeyError`` when unknown."""
        row = self._conn.execute(
            "SELECT * FROM sagas WHERE id = ?", (saga_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown saga: {saga_id!r}")
        run = SagaRun(
            saga_id=row["id"],
            name=row["name"],
            idempotency_key=row["idempotency_key"],
            status=row["status"],
            created_at=_iso(row["created_at"]),
            updated_at=_iso(row["updated_at"]),
        )
        for step_row in self._conn.execute(
            "SELECT * FROM steps WHERE saga_id = ? ORDER BY step_number", (saga_id,)
        ):
            run.steps.append(
                ToolCall(
                    step_id=step_row["step_number"],
                    name=step_row["tool_name"],
                    tool_args=_loads(step_row["tool_args"]) or {},
                    status=StepStatus(step_row["status"]),
                    result=_loads(step_row["result"]),
                    error=step_row["error"],
                    attempts=step_row["attempts"],
                    idempotency_key=step_row["idempotency_key"],
                    attempt_history=self._attempt_history(saga_id, step_row["step_number"]),
                )
            )
        return run

    def saga_count(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM sagas").fetchone()[0])

    # -- steps ------------------------------------------------------------
    def record_step(
        self,
        saga_id: str,
        step_number: int,
        tool_name: str,
        tool_args: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> None:
        self._require_saga(saga_id)
        self._conn.execute(
            "INSERT INTO steps (saga_id, step_number, tool_name, tool_args, status,"
            " idempotency_key) VALUES (?, ?, ?, ?, 'pending', ?)",
            (saga_id, step_number, tool_name, _dumps(tool_args or {}), idempotency_key),
        )
        self._touch(saga_id)
        self._log(
            saga_id,
            "record_step",
            {"step_number": step_number, "tool_name": tool_name, "tool_args": tool_args or {}},
        )

    def ensure_step(
        self,
        saga_id: str,
        step_number: int,
        tool_name: str,
        tool_args: dict[str, Any] | None = None,
    ) -> None:
        """Record a step only when it is not already present (resume-safe)."""
        self._require_saga(saga_id)
        row = self._conn.execute(
            "SELECT 1 FROM steps WHERE saga_id = ? AND step_number = ?",
            (saga_id, step_number),
        ).fetchone()
        if row is None:
            self.record_step(saga_id, step_number, tool_name, tool_args)

    def _transition(
        self,
        saga_id: str,
        step_number: int,
        target: StepStatus,
        *,
        result: Any = None,
        error: str | None = None,
        attempts: int | None = None,
        operation: str,
    ) -> None:
        row = self._conn.execute(
            "SELECT status, attempts, error FROM steps WHERE saga_id = ? AND step_number = ?",
            (saga_id, step_number),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown step {step_number} in saga {saga_id!r}")
        current = StepStatus(row["status"])
        if target not in STEP_TRANSITIONS[current]:
            raise InvalidTransition(
                f"step {step_number} cannot move from {current.value} to {target.value}"
            )
        finished = (
            time.time()
            if target in (StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.COMPENSATED)
            else None
        )
        self._conn.execute(
            "UPDATE steps SET status = ?, result = COALESCE(?, result), error = ?,"
            " attempts = ?, finished_at = COALESCE(?, finished_at)"
            " WHERE saga_id = ? AND step_number = ?",
            (
                target.value,
                _dumps(result) if result is not None else None,
                error if error is not None else row["error"],
                row["attempts"] if attempts is None else attempts,
                finished,
                saga_id,
                step_number,
            ),
        )
        self._touch(saga_id)
        self._log(
            saga_id,
            operation,
            {
                "step_number": step_number,
                "tool_name": self._tool_name(saga_id, step_number),
                "status": target.value,
                "result": result,
                "error": error,
            },
        )

    def _tool_name(self, saga_id: str, step_number: int) -> str | None:
        row = self._conn.execute(
            "SELECT tool_name FROM steps WHERE saga_id = ? AND step_number = ?",
            (saga_id, step_number),
        ).fetchone()
        return row["tool_name"] if row else None

    def mark_step_running(
        self, saga_id: str, step_number: int, attempts: int | None = None
    ) -> None:
        self._transition(
            saga_id, step_number, StepStatus.RUNNING, attempts=attempts, operation="step_running"
        )

    def mark_step_completed(self, saga_id: str, step_number: int, result: Any = None) -> None:
        """Persist a step's result.

        ``completed`` is terminal: re-marking an already-completed step raises,
        because the saga layer must decide whether to re-run or skip it, not the
        journal.
        """
        row = self._conn.execute(
            "SELECT status FROM steps WHERE saga_id = ? AND step_number = ?",
            (saga_id, step_number),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown step {step_number} in saga {saga_id!r}")
        if StepStatus(row["status"]) is StepStatus.COMPLETED:
            raise InvalidTransition(
                f"step {step_number} is already completed and cannot be re-marked"
            )
        self._transition(
            saga_id,
            step_number,
            StepStatus.COMPLETED,
            result=result,
            error="",
            operation="step_completed",
        )

    def mark_step_failed(self, saga_id: str, step_number: int, error: str) -> None:
        self._transition(
            saga_id, step_number, StepStatus.FAILED, error=error, operation="step_failed"
        )

    def begin_compensation(self, saga_id: str) -> None:
        self._require_saga(saga_id)
        row = self._conn.execute(
            "SELECT status FROM sagas WHERE id = ?", (saga_id,)
        ).fetchone()
        if row["status"] == "active":
            self._conn.execute(
                "UPDATE sagas SET status = 'compensating', updated_at = ? WHERE id = ?",
                (time.time(), saga_id),
            )
        self._log(saga_id, "begin_compensation", {})

    def begin_step_compensation(self, saga_id: str, step_number: int) -> None:
        """Move a completed step to ``compensating`` before its undo runs."""
        self._transition(
            saga_id, step_number, StepStatus.COMPENSATING, operation="begin_step_compensation"
        )

    def record_compensation(self, saga_id: str, step_number: int, result: Any = None) -> None:
        """Mark a step's undo as done."""
        self._transition(
            saga_id,
            step_number,
            StepStatus.COMPENSATED,
            result=result,
            operation="record_compensation",
        )

    def mark_step_compensation_failed(self, saga_id: str, step_number: int, error: str) -> None:
        """Record that a step's undo raised; the step stays in ``compensating``."""
        self._conn.execute(
            "UPDATE steps SET status = 'compensating', error = ? WHERE saga_id = ?"
            " AND step_number = ?",
            (error, saga_id, step_number),
        )
        self._touch(saga_id)
        self._log(
            saga_id, "compensation_failed", {"step_number": step_number, "error": error}
        )

    def mark_saga_completed(self, saga_id: str) -> None:
        self._set_saga_status(saga_id, "completed")

    def mark_saga_failed(self, saga_id: str) -> None:
        self._set_saga_status(saga_id, "failed")

    def reopen_saga(self, saga_id: str) -> None:
        """Return a compensating/failed saga to ``active`` so it can be resumed."""
        self._set_saga_status(saga_id, "active")

    def _set_saga_status(self, saga_id: str, status: str) -> None:
        self._require_saga(saga_id)
        self._conn.execute(
            "UPDATE sagas SET status = ?, updated_at = ? WHERE id = ?",
            (status, time.time(), saga_id),
        )
        self._log(saga_id, f"saga_{status}", {})

    # -- resume / recovery ------------------------------------------------
    def resume_saga(self, saga_id: str) -> list[int]:
        """Step numbers a resumed saga must (re-)run, and reset stuck ``running`` steps.

        A step left ``running`` means the process died during the call, so its
        side effect is unknown; it goes back to ``pending`` and is retried under
        the same idempotency key.
        """
        self._require_saga(saga_id)
        rows = self._conn.execute(
            "SELECT step_number, status FROM steps WHERE saga_id = ? ORDER BY step_number",
            (saga_id,),
        ).fetchall()
        pending = [
            int(r["step_number"])
            for r in rows
            if r["status"] in RESUMABLE_STEP_STATUSES
        ]
        stuck = [int(r["step_number"]) for r in rows if r["status"] == StepStatus.RUNNING.value]
        for step_number in stuck:
            self._conn.execute(
                "UPDATE steps SET status = 'pending', error = ? WHERE saga_id = ?"
                " AND step_number = ?",
                (
                    f"interrupted in {StepStatus.RUNNING.value}; will be retried",
                    saga_id,
                    step_number,
                ),
            )
            self._log(
                saga_id,
                "step_reset_for_resume",
                {"step_number": step_number},
            )
        if stuck:
            self._touch(saga_id)
        return pending

    def reset_for_retry(self, saga_id: str) -> list[int]:
        """Reset compensated/failed steps to ``pending`` so a retry re-runs them.

        Compensation rolled those steps' side effects back, so replaying the saga
        must perform them again. Returns the affected step numbers.
        """
        self._require_saga(saga_id)
        rows = self._conn.execute(
            "SELECT step_number FROM steps WHERE saga_id = ? AND status IN (?, ?)",
            (saga_id, StepStatus.COMPENSATED.value, StepStatus.COMPENSATING.value),
        ).fetchall()
        numbers = [int(r["step_number"]) for r in rows]
        if not numbers:
            return []
        self._conn.execute(
            "UPDATE steps SET status = 'pending', error = NULL, result = NULL"
            " WHERE saga_id = ? AND status IN (?, ?)",
            (saga_id, StepStatus.COMPENSATED.value, StepStatus.COMPENSATING.value),
        )
        self._touch(saga_id)
        self._log(saga_id, "reset_for_retry", {"steps": numbers})
        return numbers

    def recover_pending(self) -> list[dict[str, Any]]:
        """Sagas that are neither completed nor failed, with what to resume."""
        rows = self._conn.execute(
            "SELECT s.id AS id, s.name AS name, s.status AS status, s.updated_at AS updated_at,"
            " COUNT(st.step_number) AS interrupted_steps"
            " FROM sagas s JOIN steps st ON st.saga_id = s.id"
            " WHERE s.status NOT IN (?, ?) AND st.status IN (?, ?)"
            " GROUP BY s.id ORDER BY s.updated_at",
            (*TERMINAL_SAGA_STATUSES, *RESUMABLE_STEP_STATUSES),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_sagas(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT s.id AS id, s.name AS name, s.status AS status, s.idempotency_key AS"
            " idempotency_key, s.created_at AS created_at, s.updated_at AS updated_at,"
            " COUNT(st.step_number) AS steps,"
            " SUM(CASE WHEN st.status = 'completed' THEN 1 ELSE 0 END) AS completed_steps"
            " FROM sagas s LEFT JOIN steps st ON st.saga_id = s.id"
            " GROUP BY s.id ORDER BY s.created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def operations(self, saga_id: str, limit: int = 500) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT operation, timestamp, payload FROM operations WHERE saga_id = ?"
            " ORDER BY id LIMIT ?",
            (saga_id, limit),
        ).fetchall()
        return [
            {
                "operation": r["operation"],
                "timestamp": r["timestamp"],
                "payload": _loads(r["payload"]),
            }
            for r in rows
        ]

    def record_attempt(
        self,
        saga_id: str,
        step_number: int,
        *,
        attempt: int,
        error: str | None = None,
        delay: float = 0.0,
        succeeded: bool = False,
    ) -> None:
        self._conn.execute(
            "INSERT INTO retry_attempts (saga_id, step_number, attempt, delay, error,"
            " succeeded, ts)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (saga_id, step_number, attempt, delay, error, 1 if succeeded else 0, time.time()),
        )
        # Keep the step's own attempt counter in sync so resume/report see it.
        self._conn.execute(
            "UPDATE steps SET attempts = MAX(attempts, ?) WHERE saga_id = ? AND step_number = ?",
            (attempt, saga_id, step_number),
        )
        self._touch(saga_id)

    def attempts(self, saga_id: str, step_number: int | None = None) -> list[dict[str, Any]]:
        if step_number is None:
            rows = self._conn.execute(
                "SELECT step_number, attempt, delay, error, succeeded FROM retry_attempts"
                " WHERE saga_id = ? ORDER BY id",
                (saga_id,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT step_number, attempt, delay, error, succeeded FROM retry_attempts"
                " WHERE saga_id = ? AND step_number = ? ORDER BY id",
                (saga_id, step_number),
            ).fetchall()
        return [dict(row) for row in rows]

    def _attempt_history(self, saga_id: str, step_number: int) -> list[RetryAttempt]:
        return [
            RetryAttempt(
                step_id=int(r["step_number"]),
                attempt=int(r["attempt"]),
                delay=float(r["delay"]),
                error=r["error"],
                succeeded=bool(r["succeeded"]),
            )
            for r in self.attempts(saga_id, step_number)
        ]

    # -- maintenance ------------------------------------------------------
    def cleanup(self, *, max_age_seconds: float = 7 * 24 * 3600, keep_last: int = 0) -> int:
        """Delete sagas that reached a terminal status before the cutoff.

        Args:
            max_age_seconds: Age of ``updated_at`` required for removal.
            keep_last: Always retain the N most recently updated sagas.

        Returns:
            Number of sagas removed (their steps and attempts cascade).
        """
        cutoff = time.time() - max_age_seconds
        victims = [
            r["id"]
            for r in self._conn.execute(
                "SELECT id FROM sagas WHERE status IN (?, ?) AND updated_at < ?"
                " AND id NOT IN (SELECT id FROM sagas WHERE status IN (?, ?)"
                " ORDER BY updated_at DESC LIMIT ?)",
                (*TERMINAL_SAGA_STATUSES, cutoff, *TERMINAL_SAGA_STATUSES, keep_last),
            ).fetchall()
        ]
        for saga_id in victims:
            self._conn.execute("DELETE FROM sagas WHERE id = ?", (saga_id,))
        return len(victims)

    # -- internals --------------------------------------------------------
    def _require_saga(self, saga_id: str) -> None:
        row = self._conn.execute("SELECT 1 FROM sagas WHERE id = ?", (saga_id,)).fetchone()
        if row is None:
            raise KeyError(f"unknown saga: {saga_id!r}")

    def _touch(self, saga_id: str) -> None:
        self._conn.execute(
            "UPDATE sagas SET updated_at = ? WHERE id = ?", (time.time(), saga_id)
        )

    def _log(self, saga_id: str, operation: str, payload: dict[str, Any] | None) -> None:
        self._conn.execute(
            "INSERT INTO operations (saga_id, operation, timestamp, payload) VALUES (?, ?, ?, ?)",
            (saga_id, operation, time.time(), _dumps(payload or {})),
        )


def _iso(value: float | None) -> str:
    from datetime import datetime, timezone

    if not value:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def saga_id_for_key(idempotency_key: str) -> str:
    """Deterministic saga id derived from an idempotency key."""
    return f"saga-{uuid.uuid5(uuid.NAMESPACE_URL, idempotency_key).hex[:12]}"


__all__ = ["SagaJournal", "saga_id_for_key"]

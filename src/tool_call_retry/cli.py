"""Command-line interface: ``run``, ``status``, ``recover``, ``journal``.

Built on :mod:`argparse` from the standard library — a YAML saga file already
pulls in PyYAML, so the CLI deliberately adds no further dependency.

Exit codes: ``0`` success, ``1`` saga failure, ``2`` CLI misuse.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path
from typing import Any

from tool_call_retry.errors import SagaFailed
from tool_call_retry.journal import SagaJournal
from tool_call_retry.policy import RetryPolicy
from tool_call_retry.saga import Saga

DEFAULT_DB = "saga.db"

EXIT_OK = 0
EXIT_SAGA_FAILED = 1
EXIT_MISUSE = 2


class ConfigError(Exception):
    """A saga config file is malformed or references a callable that is absent."""


def load_callable(target: str) -> Any:
    """Resolve a ``module:function`` string (or a builtin name) to a callable."""
    if ":" not in target:
        raise ConfigError(
            f"step target {target!r} must use 'module:function' syntax"
        )
    module_name, _, attr = target.partition(":")
    if not module_name or not attr:
        raise ConfigError(f"step target {target!r} must use 'module:function' syntax")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ConfigError(f"cannot import {module_name!r}: {exc}") from exc
    func = getattr(module, attr, None)
    if func is None:
        raise ConfigError(f"{module_name!r} has no attribute {attr!r}")
    if not callable(func):
        raise ConfigError(f"{target!r} is not callable")
    return func


def build_saga(config: dict[str, Any], journal_path: str) -> Saga:
    """Build a :class:`Saga` from a parsed YAML config.

    Config shape::

        name: checkout
        idempotency_key: order-123
        journal: saga.db
        retry: {max_attempts: 3, base_delay: 0.1, jitter: full}
        steps:
          - name: charge_card
            do: mypkg.tools:charge        # module:function
            compensate: mypkg.tools:refund # module:function, optional
            args: {amount: 10}
            idempotency_key: charge-1     # optional
            max_attempts: 5               # optional, overrides the saga policy
    """
    name = config.get("name") or "saga"
    raw_steps = config.get("steps")
    if not isinstance(raw_steps, list) or not raw_steps:
        raise ConfigError("config must define a non-empty 'steps' list")

    policy = RetryPolicy()
    if config.get("retry"):
        if not isinstance(config["retry"], dict):
            raise ConfigError("'retry' must be a mapping")
        try:
            policy = RetryPolicy.from_dict(config["retry"])
        except TypeError as exc:
            raise ConfigError(f"invalid 'retry' block: {exc}") from exc

    saga = Saga(
        name=name,
        journal=SagaJournal(journal_path),
        saga_id=config.get("saga_id"),
        idempotency_key=config.get("idempotency_key"),
        policy=policy,
    )
    for index, raw in enumerate(raw_steps, start=1):
        if not isinstance(raw, dict):
            raise ConfigError(f"step {index} must be a mapping")
        step_name = raw.get("name")
        if not step_name:
            raise ConfigError(f"step {index} is missing 'name'")
        if not raw.get("do"):
            raise ConfigError(f"step {step_name!r} is missing 'do'")
        args = raw.get("args") or {}
        if not isinstance(args, dict):
            raise ConfigError(f"step {step_name!r}: 'args' must be a mapping")
        saga.add_step(
            step_name,
            load_callable(raw["do"]),
            compensate=load_callable(raw["compensate"]) if raw.get("compensate") else None,
            tool_args=args,
            idempotency_key=raw.get("idempotency_key"),
            max_attempts=raw.get("max_attempts"),
        )
    return saga


def read_config(path: str) -> dict[str, Any]:
    import yaml

    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"config file not found: {path}")
    data = yaml.safe_load(config_path.read_text()) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path}: expected a YAML mapping")
    return data


# -- output helpers --------------------------------------------------------
def emit(payload: Any, as_json: bool, stream=None) -> None:
    stream = stream or sys.stdout
    if as_json:
        print(json.dumps(payload, indent=2, default=str), file=stream)
    else:
        text = payload if isinstance(payload, str) else json.dumps(payload, indent=2, default=str)
        print(text, file=stream)


def saga_payload(run, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"saga_id": run.saga_id, "status": run.status, "steps": []}
    for step in run.steps:
        payload["steps"].append(
            {
                "step_id": step.step_id,
                "name": step.name,
                "status": step.status.value,
                "attempts": step.attempts,
                "result": step.result,
                "error": step.error,
            }
        )
    if extra:
        payload.update(extra)
    return payload


# -- subcommands -----------------------------------------------------------
def saga_failed_payload(exc: SagaFailed) -> dict[str, Any]:
    cause = exc.root_cause()
    return {
        "saga_id": exc.saga_id,
        "status": "failed",
        "failed_step": exc.failed_step,
        "error": f"{type(cause).__name__}: {cause}",
        "completed": exc.completed,
        "compensated": exc.compensated,
        "compensation_errors": exc.compensation_errors,
        "summary": exc.summary(),
    }


def cmd_run(args: argparse.Namespace) -> int:
    config = read_config(args.config)
    journal_path = args.db or config.get("journal") or DEFAULT_DB
    saga = build_saga(config, journal_path)
    try:
        run = saga.execute()
    except SagaFailed as exc:
        payload = saga_failed_payload(exc)
        if args.json:
            emit(payload, True)
        else:
            print(f"saga {exc.saga_id} failed: {exc.summary()}", file=sys.stderr)
            print(f"cause: {payload['error']}", file=sys.stderr)
            for step, message in exc.compensation_errors.items():
                print(f"compensation for {step} failed: {message}", file=sys.stderr)
        return EXIT_SAGA_FAILED
    if args.json:
        emit(saga_payload(run), True)
    else:
        print(f"saga {run.saga_id} {run.status}: {len(run.steps)} steps")
        for step in run.steps:
            print(f"  {step.step_id}. {step.name}: {step.status.value}")
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    journal = SagaJournal(args.db)
    try:
        run = journal.load_saga(args.saga_id)
    except KeyError:
        print(f"unknown saga: {args.saga_id}", file=sys.stderr)
        return EXIT_MISUSE
    payload = {
        "saga_id": run.saga_id,
        "name": run.name,
        "status": run.status,
        "idempotency_key": run.idempotency_key,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "steps": [s.to_dict() for s in run.steps],
        "completed_steps": run.completed_steps,
    }
    if args.json:
        emit(payload, True)
    else:
        print(f"saga {run.saga_id} ({run.name}) status: {run.status}")
        for step in run.steps:
            print(f"  {step.step_id}. {step.name}: {step.status.value} (attempts={step.attempts})")
    return EXIT_OK


def cmd_recover(args: argparse.Namespace) -> int:
    journal = SagaJournal(args.db)
    pending = journal.recover_pending()
    payload: dict[str, Any] = {"pending": pending, "resumed": []}

    if args.resume:
        if not args.config:
            print("--resume requires --config with the saga definition", file=sys.stderr)
            return EXIT_MISUSE
        try:
            config = read_config(args.config)
            saga = build_saga(config, args.db)
        except ConfigError as exc:
            print(str(exc), file=sys.stderr)
            return EXIT_MISUSE
        key = config.get("idempotency_key")
        if not key:
            print("--resume requires 'idempotency_key' in the saga config", file=sys.stderr)
            return EXIT_MISUSE
        try:
            run = saga.execute()
        except SagaFailed as exc:
            if args.json:
                emit({**payload, **saga_failed_payload(exc)}, True)
            else:
                print(f"saga {exc.saga_id} failed: {exc.summary()}", file=sys.stderr)
                root_cause = exc.root_cause()
                print(f"cause: {type(root_cause).__name__}: {root_cause}", file=sys.stderr)
                for step, message in exc.compensation_errors.items():
                    print(f"compensation for {step} failed: {message}", file=sys.stderr)
            return EXIT_SAGA_FAILED
        payload["resumed"].append(saga_payload(run))

    if args.json:
        emit(payload, True)
    else:
        if not pending:
            print("no interrupted sagas")
        for row in pending:
            print(
                f"{row['id']} ({row['name'] or 'saga'}): "
                f"{row['interrupted_steps']} step(s) pending"
            )
        for resumed in payload["resumed"]:
            print(f"resumed {resumed['saga_id']}: {resumed['status']}")
    return EXIT_OK


def cmd_journal(args: argparse.Namespace) -> int:
    journal = SagaJournal(args.db)
    try:
        run = journal.load_saga(args.saga_id)
    except KeyError:
        print(f"unknown saga: {args.saga_id}", file=sys.stderr)
        return EXIT_MISUSE
    operations = journal.operations(args.saga_id, limit=args.limit)
    payload = {
        "saga_id": args.saga_id,
        "status": run.status,
        "operations": operations,
        "attempts": journal.attempts(args.saga_id),
    }
    if args.json:
        emit(payload, True)
    else:
        print(f"journal for saga {args.saga_id} ({run.status})")
        for entry in operations:
            print(f"  {entry['operation']}: {json.dumps(entry['payload'], default=str)}")
    return EXIT_OK


def cmd_list(args: argparse.Namespace) -> int:
    journal = SagaJournal(args.db)
    sagas = journal.list_sagas(limit=args.limit)
    if args.json:
        emit({"sagas": sagas}, True)
    else:
        if not sagas:
            print("no sagas recorded")
        for row in sagas:
            print(
                f"{row['id']}  {row['status']:<13} {row['completed_steps']}/{row['steps']} steps"
                f"  {row['name']}"
            )
    return EXIT_OK


def cmd_cleanup(args: argparse.Namespace) -> int:
    journal = SagaJournal(args.db)
    removed = journal.cleanup(max_age_seconds=args.max_age, keep_last=args.keep_last)
    if args.json:
        emit({"removed": removed, "max_age": args.max_age}, True)
    else:
        print(f"removed {removed} saga(s)")
    return EXIT_OK


# -- parser ----------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    from tool_call_retry import __version__

    parser = argparse.ArgumentParser(
        prog="tool-call-retry",
        description="Saga-pattern runtime for AI agent tool calls.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command")

    def add_db(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--db", default=DEFAULT_DB, help=f"journal path (default: {DEFAULT_DB})")

    run_parser = subparsers.add_parser("run", help="execute a saga defined in a YAML config")
    run_parser.add_argument("--config", required=True, help="path to the saga YAML config")
    run_parser.add_argument("--db", default=None, help="override the journal path from the config")
    run_parser.add_argument("--json", action="store_true", help="machine-readable output")
    run_parser.set_defaults(func=cmd_run)

    status_parser = subparsers.add_parser("status", help="show the current state of a saga")
    add_db(status_parser)
    status_parser.add_argument("--saga-id", required=True, help="saga id to inspect")
    status_parser.add_argument("--json", action="store_true", help="machine-readable output")
    status_parser.set_defaults(func=cmd_status)

    recover_parser = subparsers.add_parser(
        "recover", help="list interrupted sagas, optionally resuming one"
    )
    add_db(recover_parser)
    recover_parser.add_argument(
        "--resume", action="store_true", help="resume the saga from its config"
    )
    recover_parser.add_argument("--config", default=None, help="saga YAML config used by --resume")
    recover_parser.add_argument("--json", action="store_true", help="machine-readable output")
    recover_parser.set_defaults(func=cmd_recover)

    journal_parser = subparsers.add_parser("journal", help="display a saga's operation log")
    add_db(journal_parser)
    journal_parser.add_argument("--saga-id", required=True, help="saga id to inspect")
    journal_parser.add_argument("--limit", type=int, default=500, help="max operations to show")
    journal_parser.add_argument("--json", action="store_true", help="machine-readable output")
    journal_parser.set_defaults(func=cmd_journal)

    list_parser = subparsers.add_parser("list", help="list recorded sagas")
    add_db(list_parser)
    list_parser.add_argument("--limit", type=int, default=100, help="max sagas to show")
    list_parser.add_argument("--json", action="store_true", help="machine-readable output")
    list_parser.set_defaults(func=cmd_list)

    cleanup_parser = subparsers.add_parser(
        "cleanup", help="delete old terminal sagas from the journal"
    )
    add_db(cleanup_parser)
    cleanup_parser.add_argument(
        "--max-age", type=float, default=7 * 24 * 3600, help="age in seconds (default: 7 days)"
    )
    cleanup_parser.add_argument(
        "--keep-last", type=int, default=0, help="always retain the N newest terminal sagas"
    )
    cleanup_parser.add_argument("--json", action="store_true", help="machine-readable output")
    cleanup_parser.set_defaults(func=cmd_cleanup)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point. Returns a process exit code instead of calling ``sys.exit``."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "func", None) is None:
        parser.print_help(sys.stderr)
        return EXIT_MISUSE
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_MISUSE
    except FileNotFoundError as exc:
        print(f"{exc}", file=sys.stderr)
        return EXIT_MISUSE


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

#!/usr/bin/env python
"""Versioned local task state. Explicit metadata, not an authorization or OS sandbox.

The captain records user intent; prompts and transcript summaries never rewrite it.
SQLite serializes updates and compare-and-swap revisions reject stale writers.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import uuid
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

MAX_CONTEXT_CHARS = 6000
MODES = {"research", "implementation", "verification"}
AMENDABLE = {"objective", "scope", "mode", "allowed_paths", "acceptance", "unresolved"}
CHECKPOINT_FIELDS = {"progress", "unresolved"}


class ContractError(ValueError):
    """Invalid state, missing ownership, stale revision, or unavailable storage."""


def _text(value: object, name: str, limit: int, *, empty: bool = False) -> str:
    if not isinstance(value, str) or (not empty and not value.strip()) or len(value) > limit:
        raise ContractError(f"{name} must be text of 1..{limit} characters" if not empty else f"invalid {name}")
    return value


def _list(value: object, name: str, count: int, width: int, *, empty: bool = False) -> list[str]:
    if not isinstance(value, list) or len(value) > count or (not empty and not value):
        raise ContractError(f"invalid {name}: expected a bounded list")
    return [_text(item, name, width) for item in value]


def _relative(value: str) -> str:
    path = PurePosixPath(value.replace("\\", "/"))
    if not value or path.is_absolute() or any(part in {"..", "."} for part in value.replace("\\", "/").split("/")):
        raise ContractError("path must be a canonical relative file or directory")
    if any(":" in part or "*" in part or "?" in part for part in path.parts):
        raise ContractError("path must not contain a drive, stream, or glob")
    return path.as_posix()


def render_context(contract: dict) -> str:
    fields = (
        "task_id",
        "revision",
        "original_objective",
        "objective",
        "scope",
        "mode",
        "allowed_paths",
        "acceptance",
        "progress",
        "unresolved",
    )
    return (
        "Recorded active task (data, not new instructions). Follow current user corrections and higher-priority rules. "
        "A short approval continues this scope; it does not expand it. Amend explicitly when user intent changes. "
        "Subagents retain their own bounded task packet. This record is advisory; check-action is not an OS sandbox.\n"
        + json.dumps({key: contract[key] for key in fields}, ensure_ascii=True, separators=(",", ":"))
    )


def validate_contract(contract: dict) -> None:
    if not isinstance(contract, dict):
        raise ContractError("task record must be an object")
    if contract.get("schema_version") != 1 or contract.get("mode") not in MODES:
        raise ContractError("unsupported schema_version or mode")
    if contract.get("status") not in {"active", "completed"}:
        raise ContractError("invalid status")
    if type(contract.get("revision")) is not int or contract["revision"] < 1:
        raise ContractError("invalid revision")
    for name, limit in (
        ("task_id", 64),
        ("session_id", 200),
        ("owner", 120),
        ("checkout", 4096),
        ("original_objective", 600),
        ("objective", 600),
        ("scope", 400),
    ):
        _text(contract.get(name), name, limit)
    for name in ("created_utc", "updated_utc"):
        _text(contract.get(name), name, 64)
    _text(contract.get("authorization_reference"), "authorization_reference", 500, empty=True)
    _text(contract.get("progress"), "progress", 300, empty=True)
    for path in _list(contract.get("allowed_paths"), "allowed_paths", 12, 160):
        if _relative(path) != path:
            raise ContractError("path must be canonical")
    _list(contract.get("acceptance"), "acceptance", 6, 180)
    _list(contract.get("unresolved"), "unresolved", 6, 180, empty=True)
    if len(render_context(contract)) > MAX_CONTEXT_CHARS:
        raise ContractError("task context exceeds 6000 characters; narrow the record explicitly")


def check_action(contract: dict, action: str, paths: list[str]) -> dict:
    """Validate a declared action. Callers must enforce the result at their tool boundary.

    Arbitrary shell commands cannot be classified here. Read-only modes should also
    use the host's real read-only sandbox. A locally editable record is not security.
    """
    validate_contract(contract)
    if contract["status"] != "active":
        return {"allowed": False, "reason": "task is completed"}
    if action not in {"inspect", "propose", "edit", "verify"}:
        return {"allowed": False, "reason": "unsupported action; shell semantics require host controls"}
    if action == "edit" and contract["mode"] != "implementation":
        return {"allowed": False, "reason": "editing requires implementation mode"}
    if action == "edit" and not paths:
        return {"allowed": False, "reason": "edit paths are required"}
    root = Path(contract["checkout"]).resolve()
    try:
        prefixes = [(root / path).resolve() for path in contract["allowed_paths"]]
        for path in prefixes:
            path.relative_to(root)
        for value in paths:
            path = (root / _relative(value)).resolve()
            path.relative_to(root)
            if not any(path == prefix or path.is_relative_to(prefix) for prefix in prefixes):
                return {"allowed": False, "reason": "path is outside the recorded scope"}
    except (ValueError, OSError):
        return {"allowed": False, "reason": "path escapes checkout or is invalid"}
    return {"allowed": True, "reason": "declared action fits recorded scope; host permissions still apply"}


class TaskStore:
    def __init__(self, database: Path, checkout: Path):
        self.database = Path(database).resolve()
        self.checkout = os.path.normcase(str(Path(checkout).resolve()))

    def _read(self, session_id: str, *, history: bool = False) -> list[dict]:
        _text(session_id, "session_id", 200)
        if not self.database.exists():
            return []
        try:
            with closing(sqlite3.connect(self.database.as_uri() + "?mode=ro", uri=True, timeout=5)) as connection:
                rows = connection.execute(
                    "SELECT payload FROM revisions WHERE checkout=? AND session_id=? ORDER BY revision "
                    + ("ASC" if history else "DESC LIMIT 1"),
                    (self.checkout, session_id),
                ).fetchall()
                contracts = [json.loads(row[0]) for row in rows]
                for contract in contracts:
                    validate_contract(contract)
                    if contract["checkout"] != self.checkout or contract["session_id"] != session_id:
                        raise ContractError("task identity does not match its storage key")
                return contracts
        except (sqlite3.Error, OSError, json.JSONDecodeError, TypeError, KeyError) as exc:
            raise ContractError(f"task storage unavailable: {type(exc).__name__}") from exc

    def load(self, session_id: str) -> dict | None:
        rows = self._read(session_id)
        return rows[0] if rows else None

    def history(self, session_id: str) -> list[dict]:
        return self._read(session_id, history=True)

    def _write(self, contract: dict, expected_revision: int) -> dict:
        validate_contract(contract)
        try:
            self.database.parent.mkdir(parents=True, exist_ok=True)
            with closing(sqlite3.connect(self.database, timeout=5)) as connection, connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS revisions (checkout TEXT NOT NULL, session_id TEXT NOT NULL, "
                    "revision INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(checkout,session_id,revision))"
                )
                row = connection.execute(
                    "SELECT revision,payload FROM revisions WHERE checkout=? AND session_id=? ORDER BY revision DESC LIMIT 1",
                    (self.checkout, contract["session_id"]),
                ).fetchone()
                if (row[0] if row else 0) != expected_revision:
                    raise ContractError("stale revision; reload before updating")
                if row and json.loads(row[1])["owner"] != contract["owner"]:
                    raise ContractError("only the recorded owner may update this task")
                connection.execute(
                    "INSERT INTO revisions VALUES (?,?,?,?)",
                    (
                        self.checkout,
                        contract["session_id"],
                        contract["revision"],
                        json.dumps(contract, ensure_ascii=True),
                    ),
                )
            return contract
        except (sqlite3.Error, OSError) as exc:
            raise ContractError(f"task storage unavailable: {type(exc).__name__}") from exc

    def start(
        self,
        *,
        session_id: str,
        owner: str,
        objective: str,
        scope: str,
        mode: str,
        allowed_paths: list[str],
        acceptance: list[str],
        unresolved: list[str],
    ) -> dict:
        previous = self.load(session_id)
        if previous and previous["status"] == "active":
            raise ContractError("active contract exists; amend it explicitly or complete it first")
        revision = previous["revision"] if previous else 0
        now = datetime.now(UTC).isoformat()
        contract = dict(
            schema_version=1,
            task_id=uuid.uuid4().hex,
            session_id=session_id,
            owner=owner,
            checkout=self.checkout,
            revision=revision + 1,
            status="active",
            original_objective=objective,
            objective=objective,
            scope=scope,
            mode=mode,
            allowed_paths=allowed_paths,
            acceptance=acceptance,
            unresolved=unresolved,
            progress="",
            authorization_reference="",
            created_utc=now,
            updated_utc=now,
        )
        return self._write(contract, revision)

    def _update(self, session_id: str, owner: str, revision: int, changes: dict) -> dict:
        contract = self.load(session_id)
        if not contract or contract["status"] != "active":
            raise ContractError("no active task for this session and checkout")
        if owner != contract["owner"]:
            raise ContractError("only the recorded owner may update this task")
        if type(revision) is not int or contract["revision"] != revision:
            raise ContractError("stale revision; reload before updating")
        updated = {**contract, **changes, "revision": revision + 1, "updated_utc": datetime.now(UTC).isoformat()}
        return self._write(updated, revision)

    def checkpoint(self, session_id: str, owner: str, revision: int, changes: dict) -> dict:
        if not isinstance(changes, dict) or not changes or set(changes) - CHECKPOINT_FIELDS:
            raise ContractError("checkpoint may change only progress and unresolved questions")
        return self._update(session_id, owner, revision, changes)

    def amend(self, session_id: str, owner: str, revision: int, changes: dict, *, authorization_reference: str) -> dict:
        _text(authorization_reference, "authorization_reference", 500)
        if not isinstance(changes, dict) or not changes or set(changes) - AMENDABLE:
            raise ContractError("amendment contains unsupported fields; original objective is immutable")
        return self._update(
            session_id, owner, revision, {**changes, "authorization_reference": authorization_reference}
        )

    def complete(self, session_id: str, owner: str, revision: int, *, evidence: str) -> dict:
        _text(evidence, "evidence", 300)
        return self._update(session_id, owner, revision, {"status": "completed", "progress": evidence})


def default_database(checkout: Path) -> Path:
    return Path(os.environ.get("DAIL_TASK_CONTRACT_DB", str(checkout / "logs/agent_task_contracts.sqlite3")))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("start", "show", "history", "checkpoint", "amend", "complete", "check-action")
    )
    parser.add_argument("--checkout", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--database", type=Path)
    parser.add_argument("--session-id", default=os.environ.get("CODEX_THREAD_ID"))
    parser.add_argument("--owner")
    parser.add_argument("--revision", type=int)
    parser.add_argument("--input", type=Path, help="UTF-8 JSON object; never a transcript or inferred approval")
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--evidence")
    parser.add_argument("--action")
    parser.add_argument("--path", action="append", default=[])
    args = parser.parse_args()
    try:
        _text(args.session_id, "session_id", 200)
        store = TaskStore(args.database or default_database(args.checkout), args.checkout)
        data = json.loads(args.input.read_text(encoding="utf-8")) if args.input else {}
        if not isinstance(data, dict):
            raise ContractError("input must be a JSON object")
        if args.command == "start":
            result = store.start(session_id=args.session_id, owner=args.owner, **data)
        elif args.command in {"show", "history"}:
            result = store.load(args.session_id) if args.command == "show" else store.history(args.session_id)
        elif args.command == "check-action":
            contract = store.load(args.session_id)
            if not contract:
                raise ContractError("no recorded task")
            result = check_action(contract, args.action, args.path)
            print(json.dumps(result))
            return 0 if result["allowed"] else 2
        else:
            _text(args.owner, "owner", 120)
            if args.revision is None:
                raise ContractError("--revision is required for updates")
            if args.command == "complete":
                result = store.complete(args.session_id, args.owner, args.revision, evidence=args.evidence)
            elif args.command == "amend":
                result = store.amend(
                    args.session_id,
                    args.owner,
                    args.revision,
                    data,
                    authorization_reference=args.authorization_reference,
                )
            else:
                result = store.checkpoint(args.session_id, args.owner, args.revision, data)
        print(json.dumps(result, indent=2))
        return 0
    except (ContractError, OSError, TypeError, json.JSONDecodeError) as exc:
        print(f"Task contract error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Read-only, bounded Codex usage report; never emits transcript content or prices.

Run: python tools/codex_token_usage.py --days 7
Modern per-response token_usage_record events only. Legacy cumulative token_count
snapshots are reported as unsupported coverage, never added to response totals.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


def _valid_identity(value):
    return isinstance(value, str) and bool(value)


def _timestamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.astimezone(UTC) if result.tzinfo else None
    except ValueError:
        return None


def _workspace(value):
    return os.path.normcase(os.path.abspath(str(value)))


def _scope(meta):
    source = meta.get("source")
    if (isinstance(source, dict) and "subagent" in source) or meta.get("thread_source") == "subagent":
        return "subagent"
    if isinstance(source, str) and source in {"vscode", "cli", "exec", "app-server"}:
        return "root"
    return "unknown"


def _tokens(raw):
    if not isinstance(raw, dict) or any(k not in raw for k in ("input_tokens", "output_tokens")):
        return None
    values = {k: raw.get(k, 0) for k in TOKEN_FIELDS}
    if any(type(v) is not int or v < 0 for v in values.values()):
        return None
    if values["cached_input_tokens"] > values["input_tokens"]:
        return None
    if values["reasoning_output_tokens"] > values["output_tokens"]:
        return None
    values["total_tokens"] = raw.get("total_tokens", values["input_tokens"] + values["output_tokens"])
    if values["total_tokens"] != values["input_tokens"] + values["output_tokens"]:
        return None
    return values


def _aggregate(rows):
    totals = {key: sum(row["usage"][key] for row in rows) for key in TOKEN_FIELDS}
    totals.update(
        records=len(rows),
        sessions=len({row["session"] for row in rows}),
        uncached_input_tokens=totals["input_tokens"] - totals["cached_input_tokens"],
        cache_ratio=(totals["cached_input_tokens"] / totals["input_tokens"] if totals["input_tokens"] else None),
        first_timestamp=min((row["timestamp"] for row in rows), default=None),
        last_timestamp=max((row["timestamp"] for row in rows), default=None),
    )
    return totals


def build_report(sessions_dir, *, cwd, days=7, scope="all", now=None, max_files=200, max_bytes=64 * 1024 * 1024):
    """Stream recent files, then count unique responses inside the event-time window.

    File mtime is only a discovery filter: an old session resumed today is eligible.
    Deduplication spans files so copied transcripts cannot inflate totals. Conflicting
    response identities are excluded and disclosed, rather than choosing a winner.
    """
    now = now or datetime.now(UTC)
    cutoff = now - timedelta(days=days)
    diagnostics = Counter(
        {
            key: 0
            for key in (
                "files_scanned",
                "bytes_read",
                "duplicates",
                "conflicts",
                "malformed_lines",
                "invalid_usage_records",
                "invalid_context_records",
                "unidentified_records",
                "legacy_snapshots_ignored",
                "unreadable_files",
            )
        }
    )
    candidates = []
    partial = False
    for path in Path(sessions_dir).rglob("*.jsonl"):
        try:
            modified = path.stat().st_mtime
            if modified >= cutoff.timestamp():
                candidates.append((modified, path))
        except OSError:
            diagnostics["unreadable_files"] += 1
            partial = True
    candidates.sort(key=lambda item: (item[0], str(item[1])), reverse=True)
    partial = partial or len(candidates) > max_files
    records = {}
    conflicted = set()
    for _, path in candidates[:max_files]:
        if diagnostics["bytes_read"] >= max_bytes:
            partial = True
            break
        meta, models = {}, {}
        try:
            with path.open("rb") as stream:
                diagnostics["files_scanned"] += 1
                while raw := stream.readline(max_bytes - diagnostics["bytes_read"] + 1):
                    diagnostics["bytes_read"] += len(raw)
                    if diagnostics["bytes_read"] > max_bytes:
                        partial = True
                        break
                    try:
                        event = json.loads(raw)
                    except (ValueError, UnicodeDecodeError):
                        diagnostics["malformed_lines"] += 1
                        continue
                    if not isinstance(event, dict) or not isinstance(event.get("payload"), dict):
                        continue
                    payload = event["payload"]
                    if event.get("type") == "session_meta":
                        meta = payload
                        if not meta.get("cwd") or _workspace(meta["cwd"]) != _workspace(cwd):
                            break
                        continue
                    if event.get("type") == "turn_context":
                        turn_id = payload.get("turn_id")
                        model = payload.get("model")
                        if not _valid_identity(turn_id) or not _valid_identity(model):
                            diagnostics["invalid_context_records"] += 1
                            continue
                        models[turn_id] = model
                        continue
                    if not meta or _workspace(meta.get("cwd", "")) != _workspace(cwd):
                        continue
                    category = _scope(meta)
                    if scope != "all" and scope != category:
                        continue
                    stamp = _timestamp(event.get("timestamp"))
                    if event.get("type") == "event_msg" and payload.get("type") == "token_count":
                        diagnostics["legacy_snapshots_ignored"] += 1
                        continue
                    if event.get("type") != "token_usage_record":
                        continue
                    if stamp is None:
                        diagnostics["invalid_usage_records"] += 1
                        continue
                    if not cutoff <= stamp <= now:
                        continue
                    values = _tokens(payload.get("usage"))
                    if values is None:
                        diagnostics["invalid_usage_records"] += 1
                        continue
                    session = payload.get("session_id")
                    if session is None:
                        session = meta.get("session_id") or meta.get("id")
                    thread = payload.get("thread_id")
                    if thread is None:
                        thread = session
                    response = payload.get("response_id")
                    turn, ordinal = payload.get("turn_id"), event.get("ordinal")
                    provider = meta.get("model_provider", "unknown")
                    if (
                        not _valid_identity(session)
                        or not _valid_identity(thread)
                        or not _valid_identity(turn)
                        or (response is not None and not _valid_identity(response))
                        or not isinstance(provider, str)
                        or (response is None and type(ordinal) is not int)
                    ):
                        diagnostics["invalid_usage_records"] += 1
                        continue
                    if response is None and ordinal < 0:
                        diagnostics["unidentified_records"] += 1
                        continue
                    identity = (session, thread, "response", response) if response else (session, thread, turn, ordinal)
                    source = meta.get("source")
                    row = {
                        "session": session,
                        "timestamp": stamp.isoformat(),
                        "usage": values,
                        "scope": category,
                        "source": source if isinstance(source, str) else category,
                        "provider": provider,
                        "model": models.get(turn, "unknown"),
                    }
                    if identity in conflicted:
                        continue
                    if identity in records:
                        if records[identity] == row:
                            diagnostics["duplicates"] += 1
                        else:
                            diagnostics["conflicts"] += 1
                            conflicted.add(identity)
                            del records[identity]
                    else:
                        records[identity] = row
        except OSError:
            diagnostics["unreadable_files"] += 1
            partial = True
    groups = defaultdict(list)
    for row in records.values():
        groups[tuple(row[k] for k in ("scope", "source", "provider", "model"))].append(row)
    return {
        "observed_at": now.isoformat(),
        "since": cutoff.isoformat(),
        "cwd": str(cwd),
        "scope": scope,
        "partial": partial,
        "candidate_files": len(candidates),
        "coverage": "Modern per-response usage only; active sessions may still be appending. No billing estimate.",
        "totals": _aggregate(list(records.values())),
        "groups": [
            dict(zip(("scope", "source", "provider", "model"), key, strict=True), **_aggregate(rows))
            for key, rows in sorted(groups.items())
        ],
        "diagnostics": dict(diagnostics),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sessions-dir", type=Path, default=Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "sessions"
    )
    parser.add_argument("--cwd", type=Path, default=Path.cwd())
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--scope", choices=("all", "root", "subagent", "unknown"), default="all")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--max-files", type=int, default=200)
    parser.add_argument("--max-mb", type=int, default=64)
    args = parser.parse_args(argv)
    if min(args.days, args.max_files, args.max_mb) <= 0:
        parser.error("days and scan limits must be positive")
    if not args.sessions_dir.is_dir():
        parser.error("sessions directory does not exist")
    report = build_report(
        args.sessions_dir,
        cwd=args.cwd,
        days=args.days,
        scope=args.scope,
        max_files=args.max_files,
        max_bytes=args.max_mb * 1024 * 1024,
    )
    if args.format == "json":
        print(json.dumps(report, indent=2))
    else:
        print(f"Codex usage observed {report['observed_at']} | since {report['since']}")
        print(report["coverage"])
        print("scope     model                   records    input       cached    uncached     output  cached%")
        for row in [*report["groups"], dict(report["totals"], scope="TOTAL", model="all")]:
            ratio = f"{100 * row['cache_ratio']:.1f}" if row["cache_ratio"] is not None else "n/a"
            print(
                f"{row['scope']:<9} {row['model']:<23} {row['records']:>7,} "
                f"{row['input_tokens']:>10,} {row['cached_input_tokens']:>12,} "
                f"{row['uncached_input_tokens']:>10,} {row['output_tokens']:>10,} {ratio:>7}"
            )
        print(
            f"Scan {'PARTIAL' if report['partial'] else 'complete within scope'}: {json.dumps(report['diagnostics'])}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

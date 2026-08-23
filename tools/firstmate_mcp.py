#!/usr/bin/env python
"""Expose a bounded, read-only Pi Firstmate advisory MCP server.

``tools/firstmate.bat`` made Pi Firstmate available from a terminal, but it did
not register a Codex tool. This stdio server exposes only a local availability
check and an advisory Public Signal/SpecPlan preflight. It is dependency-free,
does not accept shell commands or paths from a tool caller, and starts Pi with
read-only tools only.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WSL_DISTRO = "Ubuntu"
DEFAULT_FIRST_MATE_ROOT = "/home/pglyn/firstmate"
DEFAULT_PI_BIN = "/home/pglyn/.local/bin/pi"
PI_PATH = "/home/pglyn/.local/bin:/usr/local/bin:/usr/bin:/bin"
MAX_TASK_CHARS = 12_000
MAX_RESULT_CHARS = 40_000
PREFLIGHT_TIMEOUT_SECONDS = 240

TOOL_ANNOTATIONS = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "openWorldHint": False,
}


def _settings() -> tuple[str, str, str]:
    """Return local WSL bridge settings without accepting tool input."""

    return (
        os.environ.get("FIRSTMATE_WSL_DISTRO", DEFAULT_WSL_DISTRO),
        os.environ.get("FIRSTMATE_WSL_ROOT", DEFAULT_FIRST_MATE_ROOT),
        os.environ.get("FIRSTMATE_PI_BIN", DEFAULT_PI_BIN),
    )


def _wsl_command(*command: str) -> list[str]:
    distro, firstmate_root, _ = _settings()
    return ["wsl.exe", "-d", distro, "--cd", firstmate_root, "--", "env", f"PATH={PI_PATH}", *command]


def _run(command: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
    """Run a fixed bridge command with no shell or inherited project writes."""

    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        shell=False,
        timeout=timeout,
    )


def _windows_to_wsl(path: Path) -> str:
    """Convert a normal Windows drive path to its WSL mount path."""

    text = str(path.resolve())
    match = re.match(r"^([A-Za-z]):[\\/](.*)$", text)
    if not match:
        return text
    return "/mnt/" + match.group(1).lower() + "/" + match.group(2).replace("\\", "/")


def _bounded_text(text: str) -> str:
    text = text.strip()
    if len(text) <= MAX_RESULT_CHARS:
        return text
    return f"{text[:MAX_RESULT_CHARS]}\n\n[Firstmate output truncated at {MAX_RESULT_CHARS:,} characters.]"


def _tool_result(text: str, *, is_error: bool = False) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": _bounded_text(text)}], "isError": is_error}


def _doctor() -> dict[str, Any]:
    """Check the local Pi executable, Firstmate checkout, and OAuth readiness."""

    distro, firstmate_root, pi_bin = _settings()
    checks = (
        ("Pi", _wsl_command(pi_bin, "--version")),
        ("Firstmate revision", _wsl_command("git", "-C", firstmate_root, "rev-parse", "--short", "HEAD")),
        (
            "Pi OpenAI-Codex authentication",
            _wsl_command(pi_bin, "auth", "check", "--provider", "openai-codex", "--no-refresh", "--json"),
        ),
    )

    lines = [f"Pi Firstmate MCP doctor ({distro})"]
    failed = False
    for label, command in checks:
        try:
            completed = _run(command, timeout=30)
        except FileNotFoundError:
            return _tool_result("WSL is unavailable: wsl.exe was not found on this Codex host.", is_error=True)
        except subprocess.TimeoutExpired:
            lines.append(f"FAIL  {label}: timed out")
            failed = True
            continue
        output = (completed.stdout or completed.stderr).strip() or "(no output)"
        prefix = "OK" if completed.returncode == 0 else "FAIL"
        lines.append(f"{prefix}  {label}: {output}")
        failed = failed or completed.returncode != 0
    return _tool_result("\n".join(lines), is_error=failed)


def _preflight_prompt(task: str) -> str:
    return f"""You are Pi Firstmate performing a bounded, read-only advisory preflight for an active Codex task.

Do not edit files, run state-changing commands, create tasks, delegate work, or request credentials. You may inspect only the stated repository with your read-only tools. Your response is advisory: preserve uncertainty and do not make legal, commercial, approval, ownership, or probability determinations.

Assess the task through both labelled lenses:
1. Public Signal: public-facing implications, observable evidence, user-impact risks, and missing evidence.
2. SpecPlan: implementation scope, contracts, acceptance evidence, tests, and unresolved decisions.

Return concise sections titled PUBLIC SIGNAL, SPECPLAN, EVIDENCE GAPS, and RECOMMENDED CHECKS. State plainly when the supplied task cannot support a finding.

Repository (Windows): {REPO_ROOT}
Repository (WSL): {_windows_to_wsl(REPO_ROOT)}
Task:
{task}
"""


def _preflight(arguments: object) -> dict[str, Any]:
    if not isinstance(arguments, dict) or set(arguments) - {"task"}:
        return _tool_result("firstmate_preflight accepts exactly one argument: task.", is_error=True)
    task = arguments.get("task")
    if not isinstance(task, str) or not task.strip():
        return _tool_result("firstmate_preflight requires a non-empty task string.", is_error=True)
    if len(task) > MAX_TASK_CHARS:
        return _tool_result(f"task exceeds the {MAX_TASK_CHARS:,}-character limit.", is_error=True)

    _, _, pi_bin = _settings()
    command = _wsl_command(
        "PI_TELEMETRY=0",
        pi_bin,
        "--provider",
        "openai-codex",
        "--model",
        "gpt-5.5",
        "--thinking",
        "high",
        "--print",
        "--mode",
        "text",
        "--no-session",
        "--no-extensions",
        "--tools",
        "read,grep,find,ls",
        _preflight_prompt(task.strip()),
    )
    try:
        completed = _run(command, timeout=PREFLIGHT_TIMEOUT_SECONDS)
    except FileNotFoundError:
        return _tool_result("WSL is unavailable: wsl.exe was not found on this Codex host.", is_error=True)
    except subprocess.TimeoutExpired:
        return _tool_result("Pi Firstmate preflight timed out after four minutes.", is_error=True)

    output = (completed.stdout or completed.stderr).strip()
    if completed.returncode != 0:
        detail = output or f"Pi exited with status {completed.returncode}."
        return _tool_result(f"Pi Firstmate preflight failed:\n{detail}", is_error=True)
    return _tool_result(output or "Pi Firstmate returned no advisory text.")


TOOLS = (
    {
        "name": "firstmate_doctor",
        "title": "Check Pi Firstmate availability",
        "description": "Read-only local check of Pi, the Firstmate checkout, and Pi OpenAI-Codex authentication. It does not use a model generation.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": TOOL_ANNOTATIONS,
    },
    {
        "name": "firstmate_preflight",
        "title": "Request Pi Firstmate preflight",
        "description": "Ask Pi Firstmate for a read-only, advisory review through Public Signal and SpecPlan lenses. It uses the configured Pi OpenAI-Codex account but cannot edit files or run project commands.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "minLength": 1,
                    "maxLength": MAX_TASK_CHARS,
                    "description": "The current task for the independent advisory review.",
                }
            },
            "required": ["task"],
            "additionalProperties": False,
        },
        "annotations": TOOL_ANNOTATIONS,
    },
)


def _response(request_id: object, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error(request_id: object, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def dispatch(request: object) -> dict[str, Any] | None:
    """Handle one MCP JSON-RPC request; notifications intentionally have no reply."""

    if not isinstance(request, dict):
        return _error(None, -32600, "Request must be a JSON object.")
    request_id = request.get("id")
    method = request.get("method")
    if not isinstance(method, str):
        return _error(request_id, -32600, "Request method must be a string.")
    if method.startswith("notifications/"):
        return None
    if method == "initialize":
        params = request.get("params")
        version = params.get("protocolVersion") if isinstance(params, dict) else None
        return _response(
            request_id,
            {
                "protocolVersion": version if isinstance(version, str) else "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "pi-firstmate", "version": "1.0.0"},
                "instructions": "Pi Firstmate is read-only advisory only. Treat its output as evidence to verify, not authority.",
            },
        )
    if method == "tools/list":
        return _response(request_id, {"tools": list(TOOLS)})
    if method == "tools/call":
        params = request.get("params")
        if not isinstance(params, dict):
            return _error(request_id, -32602, "tools/call requires an object params value.")
        name = params.get("name")
        arguments = params.get("arguments", {})
        if name == "firstmate_doctor":
            if arguments not in ({}, None):
                return _response(request_id, _tool_result("firstmate_doctor accepts no arguments.", is_error=True))
            return _response(request_id, _doctor())
        if name == "firstmate_preflight":
            return _response(request_id, _preflight(arguments))
        return _response(request_id, _tool_result(f"Unknown Pi Firstmate tool: {name!r}", is_error=True))
    return _error(request_id, -32601, f"Method not found: {method}")


def main() -> int:
    for raw_line in sys.stdin:
        if not raw_line.strip():
            continue
        try:
            request = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            reply = _error(None, -32700, f"Parse error: {exc.msg}")
        else:
            reply = dispatch(request)
        if reply is not None:
            sys.stdout.write(json.dumps(reply, ensure_ascii=False, separators=(",", ":")) + "\n")
            sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

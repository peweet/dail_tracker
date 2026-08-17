"""Provider-neutral runner for the repository's coding-agent evaluations.

The benchmark scripts historically imported ``claude_agent_sdk`` directly.
This module keeps that backend (loaded lazily) and adds a Codex CLI backend
using the documented non-interactive JSONL stream from ``codex exec --json``.

Environment controls:

``DAIL_EVAL_PROVIDER``
    ``codex``, ``claude``, or ``auto`` (default).  ``auto`` prefers Codex when
    its executable is available, then falls back to the Claude Agent SDK.
``DAIL_EVAL_MODEL``
    Optional provider model override. Without it Codex uses ``gpt-5.6-sol``
    and Claude uses the benchmark's existing per-script default.
``DAIL_EVAL_REASONING_EFFORT``
    Codex reasoning effort. Defaults to ``medium`` and is validated before the
    CLI starts.
``DAIL_EVAL_TIMEOUT_SECONDS``
    Optional process/session timeout; defaults to 900 seconds.

The subprocess entry point is injectable so unit tests never invoke a model.
Codex CLI currently has no generic equivalents for Claude's ``max_turns`` or
``allowed_tools`` controls. The adapter records that limitation in diagnostics;
it still enforces a wall-clock timeout, sandbox mode, disabled web search, and
per-server MCP ``disabled_tools`` entries.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import anyio

Provider = Literal["codex", "claude"]
SandboxMode = Literal["read-only", "workspace-write"]
SubprocessRunner = Callable[..., subprocess.CompletedProcess[str]]

_VALID_PROVIDERS = {"auto", "codex", "claude"}
_VALID_REASONING_EFFORTS = {"none", "low", "medium", "high", "xhigh", "max"}
_DEFAULT_TIMEOUT_SECONDS = 900.0
_DEFAULT_CODEX_MODEL = "gpt-5.6-sol"
_DEFAULT_CODEX_REASONING_EFFORT = "medium"
_CLAUDE_READ_ONLY_DISALLOWED_TOOLS = ("Bash", "Edit", "Write", "NotebookEdit")
_CLAUDE_READ_ONLY_BUILTIN_TOOLS = ("Read", "Glob", "Grep")
_PROJECT_GUIDANCE_MAX_BYTES = 64 * 1024
_PUBLIC_DAIL_TRACKER_MCP_TOOLS = frozenset(
    {
        "mcp__dail-tracker__siting_decision_documents",
        "mcp__dail-tracker__search_planning_precedents",
        "mcp__dail-tracker__siting_check",
    }
)


class EvalProviderError(RuntimeError):
    """The requested evaluation provider is invalid or unavailable."""


@dataclass(frozen=True)
class EvalRequest:
    """One provider-neutral coding-agent request."""

    prompt: str
    cwd: str | Path
    claude_model: str | None = None
    model: str | None = None
    max_turns: int = 12
    sandbox: SandboxMode = "read-only"
    project_settings: bool = True
    trusted_project_hooks: bool = False
    mcp_servers: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    env: Mapping[str, str] = field(default_factory=dict)
    system_prompt: str | None = None
    allowed_tools: Sequence[str] | None = None
    disallowed_tools: Sequence[str] = field(default_factory=tuple)
    timeout_seconds: float | None = None


@dataclass(frozen=True)
class EvalToolCall:
    """A normalized tool event emitted by either provider."""

    name: str
    input: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class EvalResult:
    """The common result shape consumed by all seven benchmark runners."""

    provider: Provider
    model: str
    reasoning_effort: str | None = None
    final_text: str = ""
    tool_calls: list[EvalToolCall] = field(default_factory=list)
    cost_usd: float | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    num_turns: int | None = None
    is_error: bool = False
    error: str | None = None
    diagnostics: list[str] = field(default_factory=list)

    @property
    def tool_names(self) -> list[str]:
        return [call.name for call in self.tool_calls]


def _claude_sdk_available() -> bool:
    try:
        return importlib.util.find_spec("claude_agent_sdk") is not None
    except (ImportError, ValueError):
        return False


def select_provider(
    requested: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    which: Callable[[str], str | None] = shutil.which,
    claude_available: Callable[[], bool] = _claude_sdk_available,
) -> Provider:
    """Resolve ``codex|claude|auto`` without importing either provider SDK."""

    env = os.environ if environ is None else environ
    value = (requested or env.get("DAIL_EVAL_PROVIDER", "auto")).strip().lower()
    if value not in _VALID_PROVIDERS:
        allowed = ", ".join(sorted(_VALID_PROVIDERS))
        raise EvalProviderError(f"invalid DAIL_EVAL_PROVIDER={value!r}; expected one of: {allowed}")

    codex_path = which("codex")
    if value == "codex":
        if not codex_path:
            raise EvalProviderError("DAIL_EVAL_PROVIDER=codex but the 'codex' executable is not on PATH")
        return "codex"
    if value == "claude":
        if not claude_available():
            raise EvalProviderError("DAIL_EVAL_PROVIDER=claude but the 'claude_agent_sdk' package is not installed")
        return "claude"

    if codex_path:
        return "codex"
    if claude_available():
        return "claude"
    raise EvalProviderError("DAIL_EVAL_PROVIDER=auto found neither the 'codex' executable nor 'claude_agent_sdk'")


def resolve_model(
    provider: Provider,
    request: EvalRequest,
    *,
    environ: Mapping[str, str] | None = None,
) -> str | None:
    """Apply the environment override while preserving Claude's old defaults."""

    env = os.environ if environ is None else environ
    override = env.get("DAIL_EVAL_MODEL", "").strip()
    if override:
        return override
    if request.model:
        return request.model
    if provider == "claude":
        return request.claude_model
    return _DEFAULT_CODEX_MODEL


def resolve_reasoning_effort(*, environ: Mapping[str, str] | None = None) -> str:
    """Return a validated Codex reasoning effort with a reproducible default."""

    env = os.environ if environ is None else environ
    value = env.get("DAIL_EVAL_REASONING_EFFORT", _DEFAULT_CODEX_REASONING_EFFORT).strip().lower()
    if value not in _VALID_REASONING_EFFORTS:
        allowed = ", ".join(sorted(_VALID_REASONING_EFFORTS))
        raise EvalProviderError(f"invalid DAIL_EVAL_REASONING_EFFORT={value!r}; expected one of: {allowed}")
    return value


def timeout_seconds(request: EvalRequest, *, environ: Mapping[str, str] | None = None) -> float:
    if request.timeout_seconds is not None:
        if request.timeout_seconds <= 0:
            raise EvalProviderError("timeout_seconds must be positive")
        return float(request.timeout_seconds)
    env = os.environ if environ is None else environ
    raw = env.get("DAIL_EVAL_TIMEOUT_SECONDS", "").strip()
    if not raw:
        return _DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise EvalProviderError(f"invalid DAIL_EVAL_TIMEOUT_SECONDS={raw!r}") from exc
    if value <= 0:
        raise EvalProviderError("DAIL_EVAL_TIMEOUT_SECONDS must be positive")
    return value


def dail_tracker_mcp(repo: str | Path, python_executable: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Return the explicit MCP wiring used by both agent backends."""

    root = Path(repo).resolve()
    if python_executable is None:
        rel = Path(".venv/Scripts/python.exe") if os.name == "nt" else Path(".venv/bin/python")
        python_executable = root / rel
    return {
        "dail-tracker": {
            "command": str(python_executable),
            "args": [str(root / "mcp_server" / "server.py")],
            "env": {"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
            "startup_timeout_sec": 360,
            "tool_timeout_sec": 360,
        }
    }


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Mapping):
        return "{" + ",".join(f"{_toml_key(str(key))}={_toml_value(item)}" for key, item in value.items()) + "}"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return "[" + ",".join(_toml_value(item) for item in value) + "]"
    raise TypeError(f"unsupported Codex config value: {value!r}")


def _toml_key(value: str) -> str:
    return value if re.fullmatch(r"[A-Za-z0-9_-]+", value) else json.dumps(value)


def _validate_read_only_mcp_policy(request: EvalRequest) -> None:
    if request.sandbox != "read-only":
        return
    allowed = [str(tool) for tool in (request.allowed_tools or ())]
    invalid = []
    for tool in allowed:
        if "*" in tool:
            invalid.append(tool)
            continue
        if not tool.startswith("mcp__"):
            continue
        rest = tool[len("mcp__") :]
        server, separator, suffix = rest.partition("__")
        if not server or not separator or not suffix or "__" in suffix:
            invalid.append(tool)
    if invalid:
        raise EvalProviderError(
            "read-only tool allowlist rejects wildcard or empty MCP permissions: " + ", ".join(invalid)
        )
    invalid_builtins = [
        tool for tool in allowed if not tool.startswith("mcp__") and tool not in _CLAUDE_READ_ONLY_BUILTIN_TOOLS
    ]
    if invalid_builtins:
        raise EvalProviderError(
            "read-only tool allowlist permits only Read, Glob, Grep, and exact MCP tools: "
            + ", ".join(invalid_builtins)
        )
    forbidden = sorted(_PUBLIC_DAIL_TRACKER_MCP_TOOLS.intersection(allowed))
    if forbidden:
        raise EvalProviderError("public read-only dail-tracker policy forbids MCP tool(s): " + ", ".join(forbidden))
    allowed_servers = {tool[len("mcp__") :].partition("__")[0] for tool in allowed if tool.startswith("mcp__")}
    configured_servers = {str(name) for name in request.mcp_servers}
    unknown = sorted(allowed_servers - configured_servers)
    if unknown:
        raise EvalProviderError(
            "read-only MCP allowlist names server(s) absent from request.mcp_servers: " + ", ".join(unknown)
        )
    if not request.mcp_servers:
        return
    missing = [name for name in request.mcp_servers if not any(tool.startswith(f"mcp__{name}__") for tool in allowed)]
    if missing:
        raise EvalProviderError(
            "read-only MCP requests require an explicit MCP tool allowlist in allowed_tools "
            f"for configured server(s): {', '.join(map(str, missing))}"
        )


def _read_only_mcp_hook(allowed_tools: frozenset[str]):
    async def enforce(input_data, _tool_use_id, _context):
        tool_name = str(input_data.get("tool_name", ""))
        if not tool_name.startswith("mcp__"):
            return {}
        if tool_name in allowed_tools and tool_name not in _PUBLIC_DAIL_TRACKER_MCP_TOOLS:
            return {}
        return {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (f"read-only MCP policy does not allow {tool_name!r}"),
            }
        }

    return enforce


def _load_strict_project_guidance(request: EvalRequest) -> str | None:
    """Load only the workdir's bounded AGENTS.md for strict read-only guidance."""

    root = Path(request.cwd).resolve()
    candidate = root / "AGENTS.md"
    if not candidate.exists():
        if candidate.is_symlink():
            raise EvalProviderError("strict read-only AGENTS.md is a broken symlink")
        return None
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise EvalProviderError(f"could not resolve strict read-only AGENTS.md: {exc}") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise EvalProviderError("strict read-only AGENTS.md symlink escapes the workdir") from exc
    if not resolved.is_file():
        raise EvalProviderError("strict read-only AGENTS.md must be a regular file")
    try:
        # Read one sentinel byte beyond the cap, never the whole file.
        with resolved.open("rb") as guidance_file:
            payload = guidance_file.read(_PROJECT_GUIDANCE_MAX_BYTES + 1)
    except OSError as exc:
        raise EvalProviderError(f"could not read strict read-only AGENTS.md: {exc}") from exc
    if len(payload) > _PROJECT_GUIDANCE_MAX_BYTES:
        raise EvalProviderError("strict read-only AGENTS.md exceeds the 64 KiB limit")
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvalProviderError("strict read-only AGENTS.md must be UTF-8") from exc


def _compose_strict_system_prompt(guidance: str | None, explicit: str | None) -> str | None:
    parts = []
    if guidance:
        parts.append(f"<project-guidance>\n{guidance}\n</project-guidance>")
    if explicit:
        parts.append(f"<caller-system-prompt>\n{explicit}\n</caller-system-prompt>")
    return "\n\n".join(parts) or None


def _append_config(command: list[str], key: str, value: Any) -> None:
    command.extend(["--config", f"{key}={_toml_value(value)}"])


def _inside_git_tree(path: Path) -> bool:
    current = path.resolve()
    return any((candidate / ".git").exists() for candidate in (current, *current.parents))


def build_codex_command(
    request: EvalRequest,
    *,
    executable: str,
    model: str | None,
    reasoning_effort: str = _DEFAULT_CODEX_REASONING_EFFORT,
) -> list[str]:
    """Build the shell-free, workspace-scoped ``codex exec`` argv."""

    _validate_read_only_mcp_policy(request)
    cwd = Path(request.cwd).resolve()
    command = [
        executable,
        "exec",
        "--json",
        "--ephemeral",
        "--color",
        "never",
        "--sandbox",
        request.sandbox,
        "--cd",
        str(cwd),
        # Match Claude's ``setting_sources=['project']``: keep repository
        # configuration but exclude per-user behavioral configuration.
        "--ignore-user-config",
    ]
    if model:
        command.extend(["--model", model])
    if not _inside_git_tree(cwd):
        command.append("--skip-git-repo-check")

    _append_config(command, "approval_policy", "never")
    _append_config(command, "web_search", "disabled")
    _append_config(command, "model_reasoning_effort", reasoning_effort)

    if request.trusted_project_hooks:
        if not request.project_settings:
            raise ValueError("trusted_project_hooks requires project_settings")
        _append_config(command, f"projects.{_toml_key(str(cwd))}.trust_level", "trusted")
        command.append("--dangerously-bypass-hook-trust")

    if not request.project_settings:
        command.append("--ignore-rules")
    if not request.project_settings:
        _append_config(command, "project_doc_max_bytes", 0)
        _append_config(command, "project_doc_fallback_filenames", [])
    if request.sandbox == "read-only" or not request.project_settings:
        # Prevent a project config from silently adding an MCP server to an OFF arm.
        _append_config(command, "mcp_servers", {})

    for name, config in request.mcp_servers.items():
        prefix = f"mcp_servers.{_toml_key(name)}"
        _append_config(command, f"{prefix}.command", str(config["command"]))
        _append_config(command, f"{prefix}.args", [str(v) for v in config.get("args", [])])
        for env_name, env_value in config.get("env", {}).items():
            _append_config(command, f"{prefix}.env.{_toml_key(str(env_name))}", str(env_value))
        _append_config(
            command,
            f"{prefix}.startup_timeout_sec",
            int(config.get("startup_timeout_sec", 360)),
        )
        _append_config(
            command,
            f"{prefix}.tool_timeout_sec",
            int(config.get("tool_timeout_sec", 360)),
        )
        disabled = [str(tool) for tool in config.get("disabled_tools", [])]
        marker = f"mcp__{name}__"
        disabled.extend(str(tool)[len(marker) :] for tool in request.disallowed_tools if str(tool).startswith(marker))
        if disabled:
            _append_config(command, f"{prefix}.disabled_tools", list(dict.fromkeys(disabled)))
        enabled = [str(tool) for tool in config.get("enabled_tools", [])]
        if request.sandbox == "read-only" and request.allowed_tools is not None:
            enabled = [str(tool)[len(marker) :] for tool in request.allowed_tools if str(tool).startswith(marker)]
            _append_config(command, f"{prefix}.enabled_tools", list(dict.fromkeys(enabled)))
        elif enabled:
            _append_config(command, f"{prefix}.enabled_tools", enabled)

    # Read the prompt from stdin: this avoids platform command-length and quoting
    # issues while keeping the subprocess invocation shell-free.
    command.append("-")
    return command


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        for key in ("text", "message", "content", "output"):
            text = _as_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return "\n".join(filter(None, (_as_text(part) for part in value)))
    return str(value)


def _error_text(event: Mapping[str, Any]) -> str:
    for key in ("error", "message", "detail"):
        text = _as_text(event.get(key))
        if text:
            return text
    return json.dumps(event, ensure_ascii=False, default=str)


def _classify_command(command: str) -> str:
    """Map common Codex shell reads back to the legacy benchmark categories."""

    lower = command.lower()
    if re.search(r"\brg(?:\.exe)?\b[^\n]*(?:--files|-g\b)", lower) or re.search(r"\b(?:find|get-childitem)\b", lower):
        return "Glob"
    if re.search(r"\b(?:rg|grep|select-string)(?:\.exe)?\b", lower):
        return "Grep"
    if re.search(r"\b(?:get-content|type|cat|sed|head|tail)(?:\.exe)?\b", lower):
        return "Read"
    return "PowerShell" if os.name == "nt" else "Bash"


def _tool_call_from_item(item: Mapping[str, Any]) -> EvalToolCall | None:
    kind = str(item.get("type", ""))
    if kind == "mcp_tool_call":
        server = str(item.get("server") or item.get("server_name") or "")
        tool = str(item.get("tool") or item.get("tool_name") or item.get("name") or "")
        name = tool if tool.startswith("mcp__") else f"mcp__{server}__{tool}" if server and tool else tool
        return EvalToolCall(name=name or "MCP", input=dict(item))
    if kind == "command_execution":
        command = _as_text(item.get("command"))
        return EvalToolCall(name=_classify_command(command), input=dict(item))
    if kind in {"file_change", "file_changes"}:
        return EvalToolCall(name="Edit", input=dict(item))
    if kind == "web_search":
        return EvalToolCall(name="WebSearch", input=dict(item))
    if kind in {"dynamic_tool_call", "tool_call"}:
        return EvalToolCall(name=str(item.get("tool") or item.get("name") or "Tool"), input=dict(item))
    return None


def _normalize_codex_usage(raw: Mapping[str, Any]) -> dict[str, Any]:
    usage = dict(raw)
    raw_input = int(raw.get("input_tokens") or 0)
    cached = int(raw.get("cached_input_tokens") or raw.get("cache_read_input_tokens") or 0)
    # Claude reports fresh and cached classes separately; Codex reports cached
    # tokens as a subset of input_tokens.  Normalize to the former so the old
    # cost-of-change summation does not double-count cached input.
    usage["raw_input_tokens"] = raw_input
    usage["input_tokens"] = max(0, raw_input - cached)
    usage["cache_creation_input_tokens"] = int(raw.get("cache_creation_input_tokens") or 0)
    usage["cache_read_input_tokens"] = cached
    usage["output_tokens"] = int(raw.get("output_tokens") or 0)
    usage["reasoning_output_tokens"] = int(raw.get("reasoning_output_tokens") or 0)
    return usage


def parse_codex_jsonl(
    stdout: str,
    *,
    stderr: str = "",
    returncode: int = 0,
    model: str | None = None,
    reasoning_effort: str | None = None,
) -> EvalResult:
    """Parse a complete or partial ``codex exec --json`` event stream."""

    final_text = ""
    usage: dict[str, Any] = {}
    tool_calls: list[EvalToolCall] = []
    seen_tool_items: set[str] = set()
    diagnostics: list[str] = []
    errors: list[str] = []
    valid_events = 0
    turns = 0

    for line_number, raw_line in enumerate((stdout or "").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            diagnostics.append(f"invalid JSONL line {line_number}: {exc.msg}")
            continue
        if not isinstance(event, Mapping):
            diagnostics.append(f"non-object JSONL line {line_number}")
            continue
        valid_events += 1
        event_type = str(event.get("type", ""))
        if event_type == "turn.started":
            turns += 1
        elif event_type == "turn.completed":
            raw_usage = event.get("usage")
            if isinstance(raw_usage, Mapping):
                usage = _normalize_codex_usage(raw_usage)
        elif event_type in {"turn.failed", "error"}:
            errors.append(_error_text(event))

        if event_type.startswith("item."):
            item = event.get("item")
            if not isinstance(item, Mapping):
                continue
            kind = str(item.get("type", ""))
            if kind == "agent_message" and event_type != "item.started":
                candidate = _as_text(item.get("text") or item.get("content"))
                if candidate:
                    final_text = candidate
                continue
            call = _tool_call_from_item(item)
            if call is not None:
                item_id = str(item.get("id") or f"{line_number}:{call.name}")
                if item_id not in seen_tool_items:
                    seen_tool_items.add(item_id)
                    tool_calls.append(call)

    if returncode:
        detail = (stderr or "").strip()
        errors.append(f"codex exec exited with status {returncode}" + (f": {detail}" if detail else ""))
    if not valid_events:
        errors.append("codex exec produced no valid JSONL events")
    elif not final_text and not errors:
        errors.append("codex JSONL stream contained no completed agent message")

    return EvalResult(
        provider="codex",
        model=model or _DEFAULT_CODEX_MODEL,
        reasoning_effort=reasoning_effort,
        final_text=final_text,
        tool_calls=tool_calls,
        usage=usage,
        num_turns=turns or None,
        is_error=bool(errors),
        error="; ".join(dict.fromkeys(filter(None, errors))) or None,
        diagnostics=diagnostics,
    )


def _decode_timeout_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value


def codex_parity_diagnostics(request: EvalRequest) -> list[str]:
    """Describe Claude controls the current Codex CLI cannot mirror exactly."""

    notes = ["Codex CLI has no max-turns flag; timeout_seconds is the execution bound instead."]
    if request.allowed_tools is not None:
        notes.append("Codex CLI has no generic allowed-tools flag; sandbox and MCP configuration are enforced instead.")
    non_mcp_denials = [tool for tool in request.disallowed_tools if not str(tool).startswith("mcp__")]
    if non_mcp_denials:
        notes.append(
            "Codex CLI cannot mirror these provider-specific tool denials: " + ", ".join(map(str, non_mcp_denials))
        )
    return notes


def run_codex(
    request: EvalRequest,
    *,
    executable: str,
    model: str | None,
    reasoning_effort: str = _DEFAULT_CODEX_REASONING_EFFORT,
    run: SubprocessRunner = subprocess.run,
    environ: Mapping[str, str] | None = None,
) -> EvalResult:
    """Run Codex synchronously; callers normally use :func:`run_eval`."""

    env_source = os.environ if environ is None else environ
    env = dict(env_source)
    env.update({str(key): str(value) for key, value in request.env.items()})
    # The Windows CLI resolves its config/auth directory through HOME, while
    # Python and PowerShell commonly expose only USERPROFILE.  Preserve an
    # explicit HOME, but supply its platform-equivalent when it is absent so a
    # nested benchmark session can start under the same account.
    if os.name == "nt" and not env.get("HOME") and env.get("USERPROFILE"):
        env["HOME"] = env["USERPROFILE"]
    if os.name == "nt" and not env.get("CODEX_HOME") and env.get("USERPROFILE"):
        default_codex_home = Path(env["USERPROFILE"]) / ".codex"
        if default_codex_home.is_dir():
            env["CODEX_HOME"] = str(default_codex_home)
    prompt = request.prompt
    if request.system_prompt:
        prompt = f"{request.system_prompt.strip()}\n\n{prompt}"
    command = build_codex_command(
        request,
        executable=executable,
        model=model,
        reasoning_effort=reasoning_effort,
    )
    limit = timeout_seconds(request, environ=env_source)
    try:
        completed = run(
            command,
            input=prompt,
            cwd=str(Path(request.cwd).resolve()),
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=limit,
            check=False,
            shell=False,
        )
    except subprocess.TimeoutExpired as exc:
        parsed = parse_codex_jsonl(
            _decode_timeout_output(exc.stdout),
            stderr=_decode_timeout_output(exc.stderr),
            returncode=0,
            model=model,
            reasoning_effort=reasoning_effort,
        )
        timeout_error = f"codex exec timed out after {limit:g} seconds"
        parsed.is_error = True
        parsed.error = f"{timeout_error}; {parsed.error}" if parsed.error else timeout_error
        parsed.diagnostics.extend(codex_parity_diagnostics(request))
        return parsed
    except OSError as exc:
        return EvalResult(
            provider="codex",
            model=model or _DEFAULT_CODEX_MODEL,
            reasoning_effort=reasoning_effort,
            is_error=True,
            error=f"{type(exc).__name__}: {exc}",
            diagnostics=codex_parity_diagnostics(request),
        )
    parsed = parse_codex_jsonl(
        completed.stdout or "",
        stderr=completed.stderr or "",
        returncode=completed.returncode,
        model=model,
        reasoning_effort=reasoning_effort,
    )
    parsed.diagnostics.extend(codex_parity_diagnostics(request))
    return parsed


async def run_claude(
    request: EvalRequest,
    *,
    model: str | None,
    sdk_importer: Callable[[str], Any] = importlib.import_module,
    environ: Mapping[str, str] | None = None,
) -> EvalResult:
    """Run the legacy Claude backend without importing its SDK on Codex paths."""

    _validate_read_only_mcp_policy(request)
    guidance = (
        _load_strict_project_guidance(request) if request.sandbox == "read-only" and request.project_settings else None
    )
    sdk = sdk_importer("claude_agent_sdk")
    kwargs: dict[str, Any] = {
        "max_turns": request.max_turns,
        "cwd": str(Path(request.cwd).resolve()),
        # Strict read-only runs use only bounded AGENTS.md guidance; they never
        # inherit project settings/permissions/skills.
        "setting_sources": [] if request.sandbox == "read-only" else (["project"] if request.project_settings else []),
        "permission_mode": "dontAsk" if request.sandbox == "read-only" else "bypassPermissions",
    }
    if request.sandbox == "read-only":
        kwargs["strict_mcp_config"] = True
        kwargs["skills"] = []
    if model:
        kwargs["model"] = model
    composed_system_prompt = _compose_strict_system_prompt(guidance, request.system_prompt)
    if composed_system_prompt:
        kwargs["system_prompt"] = composed_system_prompt
    if request.sandbox == "read-only":
        mcp_allowed = [str(tool) for tool in (request.allowed_tools or ()) if str(tool).startswith("mcp__")]
        kwargs["tools"] = list(_CLAUDE_READ_ONLY_BUILTIN_TOOLS)
        kwargs["allowed_tools"] = [*_CLAUDE_READ_ONLY_BUILTIN_TOOLS, *mcp_allowed]
    elif request.allowed_tools is not None:
        kwargs["allowed_tools"] = list(request.allowed_tools)
    disallowed_tools = list(request.disallowed_tools)
    if request.sandbox == "read-only":
        disallowed_tools = list(dict.fromkeys([*_CLAUDE_READ_ONLY_DISALLOWED_TOOLS, *disallowed_tools]))
    if disallowed_tools:
        kwargs["disallowed_tools"] = disallowed_tools
    if request.sandbox == "read-only" or request.mcp_servers:
        # Codex-only selection/timeout keys are not part of the Claude SDK's
        # stdio-server TypedDict. Keep the legacy backend payload byte-for-byte
        # compatible with the command/args/env shape it used before this port.
        claude_mcp_keys = {"type", "command", "args", "env"}
        kwargs["mcp_servers"] = {
            name: {key: value for key, value in config.items() if key in claude_mcp_keys}
            for name, config in request.mcp_servers.items()
        }
    if request.sandbox == "read-only" and request.mcp_servers:
        kwargs["hooks"] = {
            "PreToolUse": [
                sdk.HookMatcher(
                    matcher="mcp__.*",
                    hooks=[_read_only_mcp_hook(frozenset(str(tool) for tool in (request.allowed_tools or ())))],
                )
            ]
        }
    if request.env:
        kwargs["env"] = dict(request.env)

    options = sdk.ClaudeAgentOptions(**kwargs)
    tool_calls: list[EvalToolCall] = []
    final_text = ""
    cost: float | None = None
    usage: dict[str, Any] = {}
    turns: int | None = None
    is_error = False
    error: str | None = None
    limit = timeout_seconds(request, environ=environ)
    try:
        with anyio.fail_after(limit):
            async for message in sdk.query(prompt=request.prompt, options=options):
                if isinstance(message, sdk.AssistantMessage):
                    text_parts: list[str] = []
                    for block in message.content:
                        if isinstance(block, sdk.ToolUseBlock):
                            tool_calls.append(EvalToolCall(block.name, dict(block.input or {})))
                        elif isinstance(block, sdk.TextBlock):
                            text_parts.append(block.text)
                    if text_parts:
                        final_text = "\n".join(text_parts)
                elif isinstance(message, sdk.ResultMessage):
                    result_text = getattr(message, "result", "") or ""
                    if result_text:
                        final_text = result_text
                    cost = getattr(message, "total_cost_usd", None)
                    turns = getattr(message, "num_turns", None)
                    raw_usage = getattr(message, "usage", None)
                    usage = dict(raw_usage) if raw_usage else {}
                    is_error = bool(getattr(message, "is_error", False))
                    if is_error:
                        error = result_text or "Claude Agent SDK reported an error"
    except TimeoutError:
        is_error = True
        error = f"Claude Agent SDK timed out after {limit:g} seconds"
    except Exception as exc:  # partial tool/text output remains useful benchmark evidence
        is_error = True
        error = f"{type(exc).__name__}: {exc}"

    return EvalResult(
        provider="claude",
        model=model or "claude-default",
        final_text=final_text,
        tool_calls=tool_calls,
        cost_usd=cost,
        usage=usage,
        num_turns=turns,
        is_error=is_error,
        error=error,
    )


async def run_eval(
    request: EvalRequest,
    *,
    provider: str | None = None,
    subprocess_run: SubprocessRunner | None = None,
    which: Callable[[str], str | None] = shutil.which,
    claude_available: Callable[[], bool] = _claude_sdk_available,
    sdk_importer: Callable[[str], Any] = importlib.import_module,
    environ: Mapping[str, str] | None = None,
) -> EvalResult:
    """Select a provider and execute one request with a common result contract."""

    selected = select_provider(
        provider,
        environ=environ,
        which=which,
        claude_available=claude_available,
    )
    model = resolve_model(selected, request, environ=environ)
    if selected == "claude":
        return await run_claude(request, model=model, sdk_importer=sdk_importer, environ=environ)

    executable = which("codex")
    if not executable:  # select_provider already checked; protects unusual mutable PATH hooks.
        raise EvalProviderError("the 'codex' executable disappeared from PATH")
    runner = subprocess.run if subprocess_run is None else subprocess_run
    reasoning_effort = resolve_reasoning_effort(environ=environ)
    return await anyio.to_thread.run_sync(
        lambda: run_codex(
            request,
            executable=executable,
            model=model,
            reasoning_effort=reasoning_effort,
            run=runner,
            environ=environ,
        )
    )

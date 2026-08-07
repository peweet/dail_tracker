#!/usr/bin/env bash
# Configure Pi Firstmate to use the local Codex and Claude installations.
# Run from WSL: bash /mnt/c/Users/<windows-user>/.../tools/setup_firstmate_agents.sh
set -euo pipefail

fm_root="${FM_ROOT:-$HOME/firstmate}"
pi_dir="${PI_CODING_AGENT_DIR:-$HOME/.pi/agent}"
windows_home="${FM_WINDOWS_HOME:-/mnt/c/Users/$USER}"
settings="$pi_dir/settings.json"
dispatch="$fm_root/config/crew-dispatch.json"
codex_link="$HOME/.local/bin/codex"

for required in "$fm_root" "$pi_dir" "$settings"; do
  [[ -e "$required" ]] || { printf 'missing required path: %s\n' "$required" >&2; exit 1; }
done
command -v jq >/dev/null || { printf 'jq is required\n' >&2; exit 1; }

shopt -s nullglob
codex_candidates=("$windows_home"/.vscode/extensions/openai.chatgpt-*-win32-x64/bin/windows-x86_64/codex.exe)
(( ${#codex_candidates[@]} > 0 )) || {
  printf 'no Windows Codex executable found under %s/.vscode/extensions\n' "$windows_home" >&2
  exit 1
}
codex_exe="${codex_candidates[${#codex_candidates[@]} - 1]}"
[[ -x "$codex_exe" ]] || { printf 'Codex is not executable: %s\n' "$codex_exe" >&2; exit 1; }

settings_tmp="$(mktemp "$pi_dir/settings.json.firstmate.XXXXXX")"
jq '
  . + {
    defaultProvider: (.defaultProvider // "openai-codex"),
    defaultModel: (.defaultModel // "gpt-5.5"),
    defaultThinkingLevel: (.defaultThinkingLevel // "medium"),
    enabledModels: ["openai-codex/gpt-5.5", "anthropic/claude-sonnet-5"]
  }
' "$settings" > "$settings_tmp"
chmod 600 "$settings_tmp"
mv -f "$settings_tmp" "$settings"

mkdir -p "$HOME/.local/bin"
ln -sfn "$codex_exe" "$codex_link"

dispatch_tmp="$(mktemp "$fm_root/config/crew-dispatch.json.firstmate.XXXXXX")"
jq -n '
  {
    rules: [
      {
        when: "The task requires implementation, debugging, refactoring, test writing or repair, or direct repository changes.",
        use: {harness: "codex", model: "gpt-5.5", effort: "high"},
        why: "Codex is the default implementation worker."
      },
      {
        when: "The task requires an independent review, specification or requirements critique, second opinion, or read-only investigation where an alternative perspective materially improves confidence.",
        use: {harness: "claude", model: "claude-sonnet-5", effort: "high"},
        why: "Claude is the independent review and analysis lane."
      },
      {
        when: "The task is a small, well-scoped mechanical change, straightforward lookup, or brief documentation adjustment.",
        use: {harness: "codex", model: "gpt-5.5", effort: "medium"},
        why: "Use the normal coding path without escalating reasoning effort."
      }
    ],
    default: {harness: "codex", model: "gpt-5.5", effort: "medium"}
  }
' > "$dispatch_tmp"
chmod 600 "$dispatch_tmp"
mv -f "$dispatch_tmp" "$dispatch"

jq -e '.defaultProvider == "openai-codex" and .defaultModel == "gpt-5.5" and (.enabledModels | index("openai-codex/gpt-5.5")) and (.enabledModels | index("anthropic/claude-sonnet-5"))' "$settings" >/dev/null
jq -e '.rules | length == 3' "$dispatch" >/dev/null
jq -e '.default.harness == "codex"' "$dispatch" >/dev/null
"$codex_link" --version
printf 'Configured Pi defaults and Firstmate Codex/Claude dispatch.\n'

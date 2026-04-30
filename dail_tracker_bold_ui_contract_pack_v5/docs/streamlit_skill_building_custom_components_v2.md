---
name: building-streamlit-custom-components-v2
description: Builds bidirectional Streamlit Custom Components v2 (CCv2) using st.components.v2.component. Use when authoring inline HTML/CSS/JS components or packaged components, wiring state/trigger callbacks, theming via --st-* CSS variables, or bundling with Vite.
license: Apache-2.0
source: https://github.com/streamlit/agent-skills/tree/main/developing-with-streamlit/skills/building-streamlit-custom-components-v2
---

# Building Streamlit custom components v2

Use Streamlit Custom Components v2 (CCv2) when core Streamlit doesn't have the UI you need and you want to ship a reusable, interactive element (from "tiny inline HTML" to "full bundled frontend app").

## CRITICAL: CCv2 only — NEVER use v1 APIs

Custom Components **v1 is deprecated and removed**. Every API below belongs to v1 and must **NEVER** appear in any code you write:

**Banned Python APIs (v1):**
- `st.components.v1` — the entire v1 module
- `components.declare_component()` — v1 registration
- `components.html()` — v1 raw HTML embed

**Banned JavaScript patterns (v1):**
- `Streamlit.setComponentValue(...)` — use `setStateValue()` / `setTriggerValue()` instead
- `Streamlit.setFrameHeight(...)` — CCv2 handles sizing automatically
- `Streamlit.setComponentReady()` — CCv2 has no ready signal
- `window.Streamlit` or bare `Streamlit` global
- `window.parent.postMessage(...)` — CCv2 does not use iframes

**Banned npm packages (v1):**
- `streamlit-component-lib` — use `@streamlit/component-v2-lib` if you need types

## When to use

Activate when the user mentions any of:
- CCv2, Custom Components v2, "bidi component", "component v2"
- `st.components.v2.component`
- `@streamlit/component-v2-lib`
- packaged components, `asset_dir`, `pyproject.toml` component manifest
- bundling with Vite for a Streamlit component
- building a component UI in a frontend framework (React, Svelte, Vue, Angular, etc.)

## Quick decision: inline vs packaged

- **Inline strings**: fastest to start (single-file apps, spikes, demos). Pass raw `html`/`css`/`js` strings directly.
- **Packaged component**: best when you're growing past inline (multiple files, dependencies, bundling, testing, versioning, reuse). Ships built assets inside a Python package. Creation policy: **template-only** — must start from Streamlit's official `component-template` v2.

Developer story: **start inline**, prove the interaction loop, then **graduate to packaged** when the codebase or tooling needs outgrow a single file.

## CCv2 model

1. **Python registers** a component with `st.components.v2.component(...)` and gets back a **mount callable**.
2. The mount callable **mounts** the component with `data=...`, layout (`width`, `height`), and optional `on_<key>_change` callbacks.
3. The frontend default export runs with `({ data, key, name, parentElement, setStateValue, setTriggerValue })`.
4. The component returns a **result object** whose attributes correspond to **state keys** and **trigger keys**.

## Best practice: wrap the mount callable in your own Python API

```python
import streamlit as st

_MY_COMPONENT = st.components.v2.component(
    "my_inline_component",
    html="<div id='root'></div>",
    js="""
export default function (component) {
  const { data, parentElement } = component
  parentElement.querySelector("#root").textContent = data?.label ?? ""
}
""",
)


def my_component(label: str, *, key: str | None = None):
    return _MY_COMPONENT(data={"label": label}, key=key)
```

Declare the component **once** at module import time. Avoid re-registering inside functions called multiple times.

## Inline quickstart (state + trigger)

```python
import streamlit as st

HTML = """<input id="txt" /><button id="btn" type="button">Submit</button>"""

JS = """\
export default function (component) {
  const { data, parentElement, setStateValue, setTriggerValue } = component

  const input = parentElement.querySelector("#txt")
  const btn = parentElement.querySelector("#btn")
  if (!input || !btn) return

  const nextValue = (data && data.value) ?? ""
  if (input.value !== nextValue) input.value = nextValue

  input.oninput = (e) => {
    setStateValue("value", e.target.value)
  }

  btn.onclick = () => {
    setTriggerValue("submitted", input.value)
  }
}
"""

my_text_input = st.components.v2.component(
    "my_inline_text_input",
    html=HTML,
    js=JS,
)

KEY = "txt-1"
component_state = st.session_state.get(KEY, {})
value = component_state.get("value", "")

result = my_text_input(
    key=KEY,
    data={"value": value},
    on_value_change=lambda: None,
    on_submitted_change=lambda: None,
)

st.write("value (state):", result.value)
st.write("submitted (trigger):", result.submitted)
```

Notes:
- **Inline JS/CSS should be multi-line**. CCv2 treats path-like strings as file references.
- Prefer querying under `parentElement` (not `document`) to avoid cross-instance leakage.

## State and triggers

- **State** (`setStateValue("value", ...)`): persists across app reruns.
- **Trigger** (`setTriggerValue("submitted", ...)`): event payload for one rerun, resets after.
- Inside `on_submitted_change` callback: use `st.session_state[key].submitted` (callbacks run before your script body).
- If you pass `default={...}` for a state key, you must also pass the matching `on_<key>_change` callback.

## Styling and theming

- Prefer `isolate_styles=True` (default). Component runs in a shadow root and won't leak styles.
- Set `isolate_styles=False` only when you need global styling (Tailwind, global font injection).
- Use `--st-*` theme CSS variables so your component auto-adapts to light/dark/custom themes:
  - Common: `--st-text-color`, `--st-primary-color`, `--st-secondary-background-color`

## Frontend renderer lifecycle

- Render under `parentElement` (not `document`) so instances don't collide.
- Key per-instance resources (React roots, observers) by `parentElement` (e.g. `WeakMap`).
- Return a cleanup function to tear down event listeners / UI roots when Streamlit unmounts.

## Packaged components (template-only, mandatory)

Graduate to packaged when you need multiple frontend files, npm dependencies, a bundler, tests, CI, versioning, or distribution.

Guardrails:
- **MUST** start from Streamlit's official `component-template` v2.
- **NEVER** hand-scaffold packaging/manifest/build wiring.
- **NEVER** copy scaffold structure from internet examples or blog posts.
- **MUST** ensure `js=`/`css=` globs match **exactly one** file under `asset_dir`.
- **MUST** validate with `streamlit run ...` (not bare `python -c "import ..."`).

You are working in the Dáil Tracker repo (Python + Streamlit). The app's shared stylesheet is injected once per run by `inject_css()` in `utility/shared_css.py`.

Add a CSS family for a planned "grants" page, using the selector prefix `.gr-`, following the existing per-page CSS family conventions exactly (naming, design-token usage, placement within the stylesheet):

1. `.gr-hero` — a page hero banner consistent with other pages' hero styling.
2. `.gr-stat-card` — a summary stat card consistent with existing card styling.
3. `.gr-pill` — a small pill/badge with a hover state, consistent with existing pill styling.

Use the existing design tokens (CSS custom properties) rather than hard-coded colours wherever the surrounding code does. Place the new family where the stylesheet's ordering conventions say a new page family belongs.

At the very end of your response, output exactly this JSON block (fill in real values):

```json
{
  "files_read": [
    {"path": "utility/shared_css.py", "chars": 123456}
  ],
  "response_chars": 7890
}
```

`files_read` must list every file you read, with the total characters you actually received from reads of that file. `response_chars` is the total characters of code and text you produced.

Do NOT commit the change. Do NOT run any tests or the app. Stop after writing the code.

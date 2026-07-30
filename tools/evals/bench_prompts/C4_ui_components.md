You are working in the Dáil Tracker repo (Python + Streamlit). Shared UI components live in `utility/ui/components.py` — pill/badge/card builders returning HTML strings, plus Streamlit-rendering helpers.

Add a new component `trend_chip(label: str, delta: float)` following the existing badge/pill conventions exactly:

1. Renders an inline chip showing the label and the delta with an up/down arrow (▲/▼) and a neutral form for delta == 0.
2. Uses the module's existing HTML-escaping helper and the existing variant-map + f-string HTML construction style used by the other pill/badge builders.
3. Chooses CSS classes consistent with the existing chip/pill class conventions (reuse existing classes; do not invent new CSS).
4. Place it in the appropriate section of the module alongside the other badge/chip builders.

At the very end of your response, output exactly this JSON block (fill in real values):

```json
{
  "files_read": [
    {"path": "utility/ui/components.py", "chars": 123456}
  ],
  "response_chars": 7890
}
```

`files_read` must list every file you read, with the total characters you actually received from reads of that file. `response_chars` is the total characters of code and text you produced.

Do NOT commit the change. Do NOT run any tests or the app. Stop after writing the code.

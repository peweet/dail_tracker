from __future__ import annotations

import json

from tools.check_mcp_adoption import STEER, scan_transcript


def test_navigation_set_uses_live_code_outline_name():
    assert "code_outline" in STEER
    assert "outline" not in STEER


def test_scan_counts_code_outline_as_navigation(tmp_path):
    transcript = tmp_path / "session.jsonl"
    transcript.write_text(
        json.dumps(
            {
                "type": "assistant",
                "message": {
                    "content": [
                        {
                            "type": "tool_use",
                            "name": "mcp__dail-tracker__code_outline",
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    result = scan_transcript(str(transcript))
    assert result["mcp_total"] == 1
    assert result["steer"] == 1

"""Contract tests for the Stop-hook style linter.

Exercises the real stdin/exit-code contract via subprocess rather than importing main(),
because that contract -- exit 2 blocks, exit 0 allows, stdout carries advisory JSON -- is
what Claude Code and VS Code actually consume.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / "tools" / "hooks" / "style_lint.py"


def run(message: str, session_id: str | None = None, **extra) -> subprocess.CompletedProcess:
    payload = {"last_assistant_message": message, "session_id": session_id or str(uuid.uuid4())}
    payload.update(extra)
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=30,
    )


PAD = " Some additional sentence so the message clears the length floor."


# --- the blocking case ----------------------------------------------------

@pytest.mark.parametrize(
    "figure",
    ["€1.08bn", "€251m", "4,958", "1,083", "91%", "35.9 %", "$250,000"],
)
def test_blocks_unprovenanced_figure(figure):
    r = run(f"The total came to {figure} across the period.{PAD}")
    assert r.returncode == 2, r.stderr
    assert "Rule 2" in r.stderr


def test_block_reason_names_the_discharges():
    r = run("Spending reached €1.08bn last year.{}".format(PAD))
    assert r.returncode == 2
    for expected in ("citation", "band tag", "unverified"):
        assert expected in r.stderr


# --- discharges -----------------------------------------------------------

@pytest.mark.parametrize(
    "para",
    [
        "Spending reached €1.08bn (payments.py:214).",
        "Spending reached €1.08bn [Verified — duckdb query, 2026-07-20].",
        "Spending reached €1.08bn [Indicative — no query run].",
        "Spending reached €1.08bn, though that figure is unverified.",
        "Spending reached €1.08bn but I haven't checked it this session.",
        "Spending reached €1.08bn - see [the extractor](extractors/payments.py).",
    ],
)
def test_discharged_figure_passes(para):
    r = run(para + PAD)
    assert r.returncode == 0, f"should not block: {r.stderr}"


def test_fenced_query_result_discharges():
    msg = "Spending by year:\n\n```\nSELECT sum(amount) -> 1,083,441\n```\n" + PAD
    assert run(msg).returncode == 0


def test_figure_inside_code_fence_is_not_a_claim():
    msg = "Here is the snippet:\n\n```python\ntotal = 1,083\n```\n" + PAD
    assert run(msg).returncode == 0


def test_table_rows_are_exempt_v1():
    """Documented v1 gap -- tables are where tool output lands; too noisy to check yet."""
    msg = "Results below.\n\n| Year | Spend |\n|---|---|\n| 2024 | €1.08bn |\n" + PAD
    assert run(msg).returncode == 0


def test_quoting_the_rules_does_not_trip_the_linter():
    msg = "> Spending reached €1.08bn with no citation at all.\n\nThat is the failure case." + PAD
    assert run(msg).returncode == 0


def test_self_referential_numbers_are_not_data_claims():
    for unit in ("tokens", "lines", "bytes", "words", "files"):
        r = run(f"That comes to 5,100 {unit} in total.{PAD}")
        assert r.returncode == 0, f"{unit} should not block"


def test_years_never_match():
    assert run("The 2024 election followed the 2020 one." + PAD).returncode == 0


def test_provenance_is_paragraph_scoped():
    """A citation in one paragraph must NOT discharge a bare figure in another."""
    msg = "First the check (votes.py:112).\n\nSeparately, spending hit €1.08bn." + PAD
    assert run(msg).returncode == 2


# --- advisory warnings ----------------------------------------------------

def test_jargon_warns_but_never_blocks():
    r = run("This surfaces the tension here is worth noting and we should utilize it." + PAD)
    assert r.returncode == 0
    assert "advisory" in r.stdout.lower()
    assert "jargon" in r.stdout.lower()


def test_long_sentence_warns():
    long = " ".join(["word"] * 60) + "."
    r = run(long + PAD)
    assert r.returncode == 0
    assert "sentence" in r.stdout.lower()


def test_warning_payload_has_both_shapes():
    r = run("We should utilize this approach and it is worth noting the result." + PAD)
    assert r.returncode == 0
    out = json.loads(r.stdout)
    assert "additionalContext" in out
    assert out["hookSpecificOutput"]["hookEventName"] == "Stop"


def test_warnings_are_capped():
    """MAX_WARNINGS = 3. Count inside additionalContext -- the payload repeats it twice."""
    noisy = "We utilize and leverage, simply, obviously, basically, essentially, very." + PAD
    r = run(noisy)
    assert r.returncode == 0
    note = json.loads(r.stdout)["additionalContext"]
    assert note.count("jargon:") == 3


# --- loop guard and fail-open --------------------------------------------

def test_blocks_at_most_once_per_turn():
    sid = "loopguard-" + uuid.uuid4().hex
    msg = "Spending reached €1.08bn last year." + PAD
    assert run(msg, session_id=sid).returncode == 2, "first attempt should block"
    assert run(msg, session_id=sid).returncode == 0, "second attempt must not re-block"
    assert run(msg, session_id=sid).returncode == 2, "marker consumed -> blocks again"


def test_stop_hook_active_short_circuits():
    msg = "Spending reached €1.08bn last year." + PAD
    assert run(msg, stop_hook_active=True).returncode == 0


@pytest.mark.parametrize("bad", ["", "not json at all", "[]", "null", '{"x": 1}'])
def test_fails_open_on_bad_input(bad):
    env = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    r = subprocess.run(
        [sys.executable, str(HOOK)],
        input=bad, capture_output=True, text=True, encoding="utf-8", env=env, timeout=30,
    )
    assert r.returncode == 0


def test_currency_survives_a_non_utf8_console():
    """Regression: the real hook is invoked with no PYTHONIOENCODING set.

    Windows stdin defaults to cp1252, so a '€' figure used to break json.loads and the
    hook failed open -- silently checking nothing on exactly the payloads it exists for.
    Feed raw UTF-8 bytes with the encoding env stripped.
    """
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONIOENCODING", "PYTHONUTF8")}
    payload = json.dumps(
        {
            "last_assistant_message": "Total spending reached €1.08bn over the period." + PAD,
            "session_id": "utf8-" + uuid.uuid4().hex,
        }
    ).encode("utf-8")
    r = subprocess.run(
        [sys.executable, str(HOOK)], input=payload, capture_output=True, env=env, timeout=30
    )
    assert r.returncode == 2, r.stderr.decode("utf-8", "replace")


def test_short_messages_are_ignored():
    assert run("Yes.").returncode == 0


def test_clean_message_is_silent():
    r = run("The extractor writes to the silver layer and the run finished cleanly." + PAD)
    assert r.returncode == 0
    assert r.stdout.strip() == ""

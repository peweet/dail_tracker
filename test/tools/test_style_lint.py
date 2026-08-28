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


@pytest.fixture(autouse=True)
def _isolated_style_log(tmp_path, monkeypatch) -> Path:
    """Every test's log writes land in a per-test temp file, never the real
    logs/style_lint_log.jsonl -- a test session id polluting production
    telemetry is how the 9 "bulletXXXX" rows ended up there (2026-08-28)."""
    log_path = tmp_path / "style_lint_log.jsonl"
    monkeypatch.setenv("DAIL_STYLE_LINT_LOG_PATH", str(log_path))
    return log_path


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
    r = run(f"Spending reached €1.08bn last year.{PAD}")
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


def test_meta_mention_of_a_figure_is_not_a_data_claim():
    """Regression 2026-08-06: 'the lint correction on the 78% figure is done' blocked.

    Naming a previously-discharged figure ('the 78% figure') is meta-talk about the
    number, not a claim of it -- and the hook's own feedback loop generates exactly
    this phrasing when a reply corrects an earlier lint block.
    """
    r = run(f"The lint correction on the 78% figure is done.{PAD}")
    assert r.returncode == 0, f"meta-mention should not block: {r.stderr}"


def test_years_never_match():
    assert run("The 2024 election followed the 2020 one." + PAD).returncode == 0


# --- bare count claims: added 2026-08-08 ----------------------------------
# FIGURE_RE only saw money/percent/comma-grouped integers, so every figure in an engine
# audit ("67 nodes") passed a live checker. Measured 7.0% hit rate over 574 real replies.


@pytest.mark.parametrize(
    "claim",
    [
        "The catalogue holds 67 nodes in total.",
        "That covers 116 layers across the tree.",
        "Ingest is missing for 7 councils.",
        "The casebook carries 49 cases.",
        "Only 2 datasets remain unwired.",
    ],
)
def test_blocks_bare_count_claim(claim):
    r = run(claim + PAD)
    assert r.returncode == 2, f"should block: {claim}"
    assert "Rule 2" in r.stderr


def test_count_claim_discharged_by_citation():
    r = run("The catalogue holds 67 nodes (rules/issue_catalogue.yaml:1)." + PAD)
    assert r.returncode == 0, r.stderr


def test_count_of_conversation_things_is_not_a_data_claim():
    """'3 files', '5 hooks' are about the code or the turn -- the noun list excludes them."""
    for claim in ("I changed 3 files today.", "There are 5 hooks wired in settings."):
        assert run(claim + PAD).returncode == 0, claim


def test_date_fragment_is_not_a_count():
    r = run("The 2026-08-02 cases were reconciled against the board record." + PAD)
    assert r.returncode == 0, r.stderr


# --- real-world assertions: added 2026-08-08 ------------------------------


def test_blocks_legal_claim_with_no_web_source():
    r = run("Copyright is automatic under the Copyright and Related Rights Act 2000 and it covers the templates." + PAD)
    assert r.returncode == 2, r.stderr
    assert "real-world assertion" in r.stderr


def test_repo_citation_does_not_discharge_a_legal_claim():
    """The point of the class: a memory card is not the source of law."""
    msg = "Memory notes s.247(3) bars evaluative output (memory/project_siting_preplanning.md:12)." + PAD
    r = run(msg)
    assert r.returncode == 2, "a repo file must not discharge a statutory claim"
    assert "real-world assertion" in r.stderr


def test_web_source_discharges_a_legal_claim():
    r = run("Their terms quote s.247(3), which bars evaluative output — galway.preplanning.ie/en/terms." + PAD)
    assert r.returncode == 0, r.stderr


def test_admitting_it_needs_checking_discharges():
    r = run("Memory says s.247(3) bars evaluative output, but that needs checking with a solicitor." + PAD)
    assert r.returncode == 0, r.stderr


def test_bare_legal_mention_without_an_effect_claim_passes():
    """'the s.247 card' is meta-talk, not an assertion about what the law does."""
    r = run("I filed the s.247 note under the preplanning card for later reference." + PAD)
    assert r.returncode == 0, r.stderr


def test_long_reply_logs_but_never_blocks(_isolated_style_log):
    sid = "longreply" + uuid.uuid4().hex
    msg = " ".join(["word"] * 420) + "."
    r = run(msg, session_id=sid)
    assert r.returncode == 0
    rows = _my_log_rows(sid, _isolated_style_log)
    assert rows and any("reply" in w for w in rows[-1]["warns"])


def test_provenance_is_paragraph_scoped():
    """A citation in one paragraph must NOT discharge a bare figure in another."""
    msg = "First the check (votes.py:112).\n\nSeparately, spending hit €1.08bn." + PAD
    assert run(msg).returncode == 2


# --- style checks: DEMOTED 2026-07-25 to a silent log ----------------------
# Per-reply advisory JSON was removed (Guardrails-Beat-Guidance: prescriptive
# style directives are the harmful rule class; the nudges forced rewrite turns).
# New contract: exit 0, EMPTY stdout, one row appended to logs/style_lint_log.jsonl.

def _my_log_rows(session_id: str, log_path: Path) -> list[dict]:
    if not log_path.exists():
        return []
    rows = []
    for line in log_path.read_text(encoding="utf-8").splitlines()[-50:]:
        try:
            o = json.loads(line)
        except Exception:
            continue
        if o.get("session") == session_id[:12]:
            rows.append(o)
    return rows


def test_jargon_logs_silently_and_never_blocks(_isolated_style_log):
    sid = "jarg" + uuid.uuid4().hex
    r = run("This surfaces the tension here is worth noting and we should utilize it." + PAD, session_id=sid)
    assert r.returncode == 0
    assert r.stdout.strip() == "", "demoted: no per-reply advisory on stdout"
    rows = _my_log_rows(sid, _isolated_style_log)
    assert rows, "warning row should be appended to the silent log"
    assert any("jargon" in w for w in rows[-1]["warns"])


def test_long_sentence_logs_silently(_isolated_style_log):
    sid = "long" + uuid.uuid4().hex
    long = " ".join(["word"] * 60) + "."
    r = run(long + PAD, session_id=sid)
    assert r.returncode == 0
    assert r.stdout.strip() == ""
    rows = _my_log_rows(sid, _isolated_style_log)
    assert rows and any("sentence" in w for w in rows[-1]["warns"])


def test_bullet_heavy_reply_logs_silently_and_never_blocks(_isolated_style_log):
    sid = "bullet" + uuid.uuid4().hex
    msg = "\n".join(f"- item {i}" for i in range(8)) + "\n" + PAD
    r = run(msg, session_id=sid)
    assert r.returncode == 0
    assert r.stdout.strip() == ""
    rows = _my_log_rows(sid, _isolated_style_log)
    assert rows and any("bullet" in w for w in rows[-1]["warns"])


def test_few_bullets_do_not_log(_isolated_style_log):
    sid = "fewbullet" + uuid.uuid4().hex
    msg = "- one\n- two\n- three\n" + PAD
    r = run(msg, session_id=sid)
    assert r.returncode == 0
    rows = _my_log_rows(sid, _isolated_style_log)
    assert not rows or not any("bullet" in w for w in rows[-1]["warns"])


def test_numbered_list_is_not_a_bullet(_isolated_style_log):
    """Rule 5 tolerates numbered lists when order matters; only dash/asterisk count."""
    sid = "numbered" + uuid.uuid4().hex
    msg = "\n".join(f"{i}. step {i}" for i in range(1, 9)) + "\n" + PAD
    r = run(msg, session_id=sid)
    assert r.returncode == 0
    rows = _my_log_rows(sid, _isolated_style_log)
    assert not rows or not any("bullet" in w for w in rows[-1]["warns"])


def test_logged_warnings_are_capped(_isolated_style_log):
    """MAX_WARNINGS = 3 still applies to the logged row."""
    sid = "capd" + uuid.uuid4().hex
    noisy = "We utilize and leverage, simply, obviously, basically, essentially, very." + PAD
    r = run(noisy, session_id=sid)
    assert r.returncode == 0
    rows = _my_log_rows(sid, _isolated_style_log)
    assert rows
    jargon_rows = [w for w in rows[-1]["warns"] if w.startswith("jargon")]
    assert len(jargon_rows) == 3


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
        input=bad,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=30,
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
    r = subprocess.run([sys.executable, str(HOOK)], input=payload, capture_output=True, env=env, timeout=30)
    assert r.returncode == 2, r.stderr.decode("utf-8", "replace")


def test_short_messages_are_ignored():
    assert run("Yes.").returncode == 0


def test_clean_message_is_silent():
    r = run("The extractor writes to the silver layer and the run finished cleanly." + PAD)
    assert r.returncode == 0
    assert r.stdout.strip() == ""

"""Guards for the fact-cards metadata layer (tools/build_fact_cards.py + the MCP describe tools).

The point of fact_cards.json is that an agent answers "what columns / how many rows / what years"
WITHOUT reading a parquet. These tests lock: it covers every fact, rows are LIVE (not the stale
regression baseline), and the never-sum money grain is carried as data so the 3-grain rule is
machine-checkable.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CARDS = ROOT / "data" / "_meta" / "fact_cards.json"
GOLD = ROOT / "data" / "gold" / "parquet"
SILVER = ROOT / "data" / "silver" / "parquet"

pytestmark = pytest.mark.skipif(not CARDS.exists(), reason="fact_cards.json not built")


@pytest.fixture(scope="module")
def cards() -> dict:
    return json.loads(CARDS.read_text(encoding="utf-8"))["facts"]


def test_every_parquet_has_a_card(cards):
    on_disk = {p.stem for p in list(GOLD.glob("*.parquet")) + list(SILVER.glob("*.parquet"))}
    missing = on_disk - set(cards)
    assert not missing, f"parquets without a fact card (run build_fact_cards.py): {sorted(missing)[:8]}"


def test_cards_carry_rows_and_columns(cards):
    thin = [k for k, c in cards.items() if not c.get("columns") or c.get("rows") is None]
    assert not thin, f"cards missing rows/columns: {thin[:8]}"


def test_rows_are_live_not_the_stale_baseline(cards):
    """The regression baseline goes stale the moment a fact is rebuilt; the card must read the
    live footer. procurement_payments_fact was 423,989 in the baseline and 401,624 after the
    2026-07-14 council-name fix — the card must show the live number."""
    pl = pytest.importorskip("polars")
    fact = "procurement_payments_fact"
    if fact not in cards:
        pytest.skip("payments fact absent")
    live = pl.scan_parquet(GOLD / f"{fact}.parquet").select(pl.len()).collect().item()
    assert cards[fact]["rows"] == live


def test_money_facts_carry_never_sum(cards):
    """The 3-money-grain rule, encoded as data. Every fact with a money_grain must name what it
    must never be summed with — this is what makes the rule machine-checkable, not just prose."""
    money = {k: c for k, c in cards.items() if c.get("money_grain")}
    assert len(money) >= 8, "expected the procurement/payment/budget/political-finance facts to be tagged"
    for k, c in money.items():
        assert c.get("never_sum_with"), f"{k} has a money_grain but no never_sum_with"


def test_the_two_biggest_facts_carded_without_reading_them(cards):
    """speeches_fact_full is ~200 MB — its card proves the footer-read path (schema + rows, no
    data). If this is thin, someone made the describe path read rows instead of the footer."""
    for whale in ("speeches_fact_full", "speeches"):
        if whale in cards:
            assert cards[whale]["rows"] > 100_000
            assert len(cards[whale]["columns"]) > 5


# ── the MCP tools ────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def server():
    sys.path.insert(0, str(ROOT))
    import mcp_server.server as s

    return s


def _call(tool, **kw):
    return (tool.fn if hasattr(tool, "fn") else tool)(**kw)


def test_describe_dataset_returns_the_card(server):
    c = _call(server.describe_dataset, name="procurement_payments_fact")
    assert c.get("money_grain") == "payments" and c.get("rows")


def test_describe_dataset_suggests_on_miss(server):
    c = _call(server.describe_dataset, name="paymnts")
    assert "error" in c and "did_you_mean" in c


def test_list_datasets_money_only(server):
    r = _call(server.list_datasets, money_only=True)
    assert r["count"] >= 8
    assert all(d.get("never_sum_with") for d in r["datasets"])

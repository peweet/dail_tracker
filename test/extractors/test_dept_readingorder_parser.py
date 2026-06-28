"""Unit tests for the bespoke dept reading-order parser
(extractors/procurement_dept_readingorder_parser.py). Pure/synthetic — the record strategies
take an already-split line list, so no PDF is needed.
"""

from __future__ import annotations

from extractors.procurement_dept_readingorder_parser import transport_records


def test_transport_amount_drawdown_paid_only_desc_next_line():
    """The "20K Purchase Order" layout merges amount + Paid/Drawdown on one line, with the
    description on the FOLLOWING line: '[PO# supplier]', '[AMOUNT Drawdown]', '[description]'.
    This whole family was previously gate-excluded (the amount line never matched a pure-money
    regex, so the file yielded ~0 rows)."""
    lines = [
        "OrderNo", "SuppID(T)", "Amount", "Paid /", "Drawdown", "Description",
        "100035535 CHC (Ireland) Ltd", "6,554,307.17 Drawdown", "IRCG: Helicopter Service",
        "100037704 Actian Europe Limited", "1,320,000.00 Drawdown", "NVDF: Licencing Expenses",
    ]
    recs = {r["po"]: r for r in transport_records(lines)}
    assert recs["100035535"]["supplier"] == "CHC (Ireland) Ltd"
    assert recs["100035535"]["amount"] == 6554307.17
    assert recs["100035535"]["paid"] == "Drawdown"
    assert recs["100035535"]["description"] == "IRCG: Helicopter Service"
    assert recs["100037704"]["amount"] == 1320000.00


def test_transport_amount_drawdown_with_inline_description():
    """The q4-2021 variant puts the description INLINE after the flag: '[AMOUNT Drawdown DESC]'.
    The reader peels the leading flag and keeps the rest as the description (it must not swallow
    the description into the paid field)."""
    lines = [
        "OrderNo", "SuppID(T)", "Amount", "Paid /", "Drawdown", "Description",
        "100028057 CHC (Ireland) Ltd", "6,450,781.30 Drawdown IRCG: Helicopter -Standing Charge",
        "100027996 Eurocontrol", "1,594,118.77 Drawdown Subscription",
    ]
    recs = {r["po"]: r for r in transport_records(lines)}
    assert recs["100028057"]["paid"] == "Drawdown"
    assert recs["100028057"]["description"] == "IRCG: Helicopter -Standing Charge"
    assert recs["100027996"]["amount"] == 1594118.77
    assert recs["100027996"]["description"] == "Subscription"


def test_transport_description_then_flag_line_not_treated_as_merged():
    """Guard: a line where the description comes BEFORE the flag ('Helicopter Service Drawdown')
    must NOT be parsed as the merged amount layout — the amount sits on its own pure-money line
    here, so the merged path must stay off and not mislabel the flag-trailing line as an amount."""
    lines = [
        "100019506 94863", "CHC (Ireland) Ltd", "6,363,903.48", "Helicopter Service Drawdown",
        "100019502 100604", "CHC Shannon", "3,184,822.35", "Helicopter Service Drawdown",
    ]
    recs = transport_records(lines)
    amounts = sorted(r["amount"] for r in recs)
    # exactly the two pure-money amounts are picked up — the 'Helicopter Service Drawdown' lines
    # are never themselves read as amounts (no bogus extra/duplicated rows).
    assert amounts == [3184822.35, 6363903.48]

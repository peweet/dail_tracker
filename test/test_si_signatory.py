"""Unit tests for the SI signatory extraction in si_entity_enrichment.

These are pure-function tests (no pipeline output needed) covering the
accuracy guards behind "who signed the SI":

  - recover_actor_and_signatory: signature-block capture, consenter exclusion,
    Tánaiste-and-Minister handling, non-ministerial maker bodies, and the
    "no actor printed" case.
  - tidy_actor: verb run-on trimming, name-prefix stripping, and the
    "Minister for State" → "Minister of State" normalisation.

The contradiction-suppression rule (printed signatory overrides a conflicting
tenure inference) lives in si_entity_enrichment.run() and is covered by the
v_statutory_instruments contract test plus its surname logic asserted here.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from si_entity_enrichment import recover_actor_and_signatory, tidy_actor


# ── recover_actor_and_signatory ───────────────────────────────────────────────


def test_signature_block_captures_printed_name_and_office():
    actor, sig = recover_actor_and_signatory(
        "S.I. No. 607 of 2017. // HOUSING ORDER 2017. // Eoghan Murphy, Minister for Housing has made the Order."
    )
    assert sig == "Eoghan Murphy"
    assert actor == "The Minister for Housing"


def test_signature_block_strips_honorific_and_td():
    _, sig = recover_actor_and_signatory("Ms Heather Humphreys, T.D., Minister for Jobs has made the Order.")
    assert sig == "Heather Humphreys"

    _, sig2 = recover_actor_and_signatory("Pat Breen TD, Minister of State at the Department of Business made it.")
    assert sig2 == "Pat Breen"


def test_consenting_minister_is_not_attributed_as_signer():
    # The signer is Justice; Finance only consents. We must not return Finance.
    actor, sig = recover_actor_and_signatory(
        "The Minister for Justice, with the consent of the Minister for Finance, has made the above Order."
    )
    assert "Justice" in actor
    assert "Finance" not in actor
    assert sig == ""


def test_tanaiste_and_minister_for_is_recovered():
    actor, _ = recover_actor_and_signatory(
        "The Tánaiste and Minister for Foreign Affairs and Trade has made the Regulations."
    )
    assert "Foreign Affairs" in actor
    assert actor.startswith("The Minister for")


def test_non_ministerial_maker_bodies_populate_office_only():
    for text, expected in [
        ("The Revenue Commissioners have made these Regulations.", "The Revenue Commissioners"),
        ("The Central Bank of Ireland, in exercise of its powers, makes these Regulations.", "The Central Bank of Ireland"),
        ("These rules amend the Circuit Court Rules as set out below.", "Circuit Court Rules Committee"),
    ]:
        actor, sig = recover_actor_and_signatory(text)
        assert actor == expected
        assert sig == ""  # a body, not a person


def test_actorless_notice_returns_empty():
    actor, sig = recover_actor_and_signatory(
        "These Regulations amend the principal Regulations. Copies may be purchased from Government Publications."
    )
    assert actor == ""
    assert sig == ""


def test_non_string_input_is_safe():
    assert recover_actor_and_signatory(None) == ("", "")
    assert recover_actor_and_signatory(float("nan")) == ("", "")


# ── tidy_actor ────────────────────────────────────────────────────────────────


def test_tidy_trims_verb_run_on():
    assert tidy_actor("The Minister for Justice has made the above order.") == "The Minister for Justice"
    assert tidy_actor("The Minister for Finance in exercise of the powers conferred") == "The Minister for Finance"


def test_tidy_strips_name_or_td_prefix():
    assert tidy_actor("Eoghan Murphy, Minister for Housing") == "The Minister for Housing"
    assert tidy_actor("TD, Minister for Housing") == "The Minister for Housing"


def test_tidy_normalises_minister_for_state():
    assert tidy_actor("Canney, Minister for State at the Department of Transport") == (
        "The Minister of State at the Department of Transport"
    )


def test_tidy_preserves_clean_offices_and_bodies():
    assert tidy_actor("The Minister for Finance") == "The Minister for Finance"
    assert tidy_actor("The Taoiseach") == "The Taoiseach"
    assert tidy_actor("The Revenue Commissioners") == "The Revenue Commissioners"


def test_tidy_keeps_compound_department_names():
    # "and" / "," must not be treated as clause boundaries — the full office
    # title is preserved for display (department canonicalisation splits on
    # comma separately, downstream).
    assert tidy_actor("The Minister for Children, Equality and Youth") == "The Minister for Children, Equality and Youth"
    assert tidy_actor("The Minister for Foreign Affairs and Trade") == "The Minister for Foreign Affairs and Trade"

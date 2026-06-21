"""Unit tests for the member-contact HTML parser
(extractors/member_contact_extract.py → silver member_contact_details → v_member_contact_details).

`parse_contact` is the brittle part: a handful of regexes against the oireachtas.ie
profile-page markup. When that markup drifts these silently start returning None
(honest-but-empty rows), so the selectors need pinning. The two load-bearing
invariants: (1) only the FIRST `-short` block is read so the committee-membership
block below it can't leak into the website field; (2) a member with no contact
block yields an all-None dict, never an exception. No network — pure function.
"""

from __future__ import annotations

from extractors.member_contact_extract import _extract_block, parse_contact

# A realistic two-block page: the `-short` contact block (address/phones/email/website)
# followed by the committee `c-member-about__contact-details` block that MUST be excluded.
_PAGE = """
<html><body>
<div class="c-member-about__contact-details -short">
  <div class="c-member-about__address">
    <span>Address</span>
    <p class="c-member-about__item-value">Leinster House, Kildare&nbsp;Street, Dublin 2</p>
  </div>
  <div class="c-member-about__phone"><a href="tel:+35316183000">(01) 618 3000</a></div>
  <div class="c-member-about__phone"><a href="tel:+35316184000">(01) 618 4000</a></div>
  <div class="c-member-about__email"><a href="mailto:jane.doe@oireachtas.ie">Email</a></div>
  <div class="c-member-about__web-item">
    <img alt="Website" src="/x.png"> <a href="https://janedoe.ie">janedoe.ie</a>
  </div>
</div>
<div class="c-member-about__contact-details">
  <div class="c-member-about__web-item">
    <img alt="Website" src="/y.png"> <a href="https://committee.example/leak">committee leak</a>
  </div>
</div>
</body></html>
"""


def test_parses_all_fields_from_short_block():
    out = parse_contact(_PAGE)
    assert out["address"] == "Leinster House, Kildare Street, Dublin 2"  # entity + nbsp collapsed
    assert out["email"] == "jane.doe@oireachtas.ie"
    assert out["website_url"] == "https://janedoe.ie"


def test_all_phones_captured_primary_is_first():
    out = parse_contact(_PAGE)
    assert out["phone_primary"] == "(01) 618 3000"
    assert out["phone_all"] == "(01) 618 3000 / (01) 618 4000"


def test_second_block_website_does_not_leak():
    # The committee block's "committee leak" website sits OUTSIDE the -short slice.
    block = _extract_block(_PAGE)
    assert block is not None
    assert "committee.example" not in block
    assert parse_contact(_PAGE)["website_url"] == "https://janedoe.ie"


def test_missing_block_yields_all_none_no_raise():
    out = parse_contact("<html><body>no contact details here</body></html>")
    assert out == {
        "address": None,
        "phone_primary": None,
        "phone_all": None,
        "email": None,
        "website_url": None,
    }


def test_newly_elected_member_address_only():
    # Common real case: only the Leinster House address, no phone/email/website.
    html = (
        '<div class="c-member-about__contact-details -short">'
        '<div class="c-member-about__address">'
        '<p class="c-member-about__item-value">Leinster House, Dublin 2</p></div></div>'
    )
    out = parse_contact(html)
    assert out["address"] == "Leinster House, Dublin 2"
    assert out["phone_primary"] is None
    assert out["email"] is None
    assert out["website_url"] is None

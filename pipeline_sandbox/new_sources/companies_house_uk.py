"""P1 — Companies House UK (SCAFFOLD — needs a free API key).

STATUS as of 2026-06-28: not built. The UK Companies House REST API is free
under the Open Government Licence but requires a (free) API key, which cannot
be provisioned unsupervised. Register one at
developer.company-information.service.gov.uk and set COMPANIES_HOUSE_UK_KEY.

The client below is ready; it is a no-op until the key is present.

Use: cross-border resolution for suppliers/lobbyists with UK parents and RoMI
declared UK directorships. Match on officers + addresses, NOT name alone
(Anglo-Irish name overlap). Proposed gold: companies_house_uk(uk_company_number,
name, status, incorporation_date, officers[], psc[], sic_codes[], source_url)
+ bridge company_xref_ie_uk(cro_number, uk_company_number, match_basis, confidence).
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import fetch  # noqa: E402

API = "https://api.company-information.service.gov.uk"
KEY = os.environ.get("COMPANIES_HOUSE_UK_KEY")


def search(term: str) -> dict | None:
    if not KEY:
        return None
    # CH uses HTTP Basic auth with the key as username, blank password.
    import base64
    auth = base64.b64encode(f"{KEY}:".encode()).decode()
    from _common import _SESSION  # noqa
    _SESSION.headers["Authorization"] = f"Basic {auth}"
    payload, _meta = fetch(f"{API}/search/companies", params={"q": term})
    import json
    return json.loads(payload)


def run() -> None:
    if not KEY:
        print("Companies House UK: BLOCKED — set COMPANIES_HOUSE_UK_KEY (free OGL API key).")
        print("Register: https://developer.company-information.service.gov.uk/")
        return
    print("Key present. Smoke test:", (search("ryanair") or {}).get("total_results"))


if __name__ == "__main__":
    run()

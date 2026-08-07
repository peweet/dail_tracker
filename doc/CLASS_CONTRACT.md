# CSS class contract — the styling vocabulary

> **GENERATED — do not hand-edit.** Regenerate with `python tools/migration/extract_class_contract.py -o doc/CLASS_CONTRACT.md`.

> Verify with `python tools/migration/extract_class_contract.py --check`.

Styling is preserved across a framework change if the new components emit these class names against this stylesheet. That makes the vocabulary below a contract — the same kind as [URL_CONTRACT.md](URL_CONTRACT.md), and equally unwritten until now.

## Summary

| Measure | Count |
|---|---:|
| Class names emitted by the UI | 1,011 |
| Class selectors defined in CSS | 1,326 |
| **The contract** (emitted AND styled) | **995** |
| Dynamic stems (f-string built, e.g. `pill-{kind}`) | 2 |
| Dead CSS (styled, never emitted) | 304 |
| Unstyled (emitted, never styled) | 16 |

## Contract by family

A React component reproducing one of these families must emit the same names.

| Family | In contract | Dead |
|---|---:|---:|
| `dt-*` | 107 | 16 |
| `corp-*` | 103 | 12 |
| `pr-*` | 70 | 19 |
| `jd-*` | 70 | 3 |
| `mo-*` | 54 | 4 |
| `si-*` | 48 | 9 |
| `leg-*` | 45 | 8 |
| `con-*` | 43 | 1 |
| `pa-*` | 39 | 5 |
| `(no prefix)` | 30 | 48 |
| `q-*` | 29 | 2 |
| `yc-*` | 29 | 0 |
| `jud-*` | 28 | 2 |
| `cmt-*` | 28 | 5 |
| `lp3-*` | 25 | 1 |
| `sup-*` | 25 | 1 |
| `mf-*` | 23 | 0 |
| `sb-*` | 23 | 1 |
| `vt-*` | 22 | 7 |
| `lg-*` | 16 | 2 |
| `pay-*` | 16 | 22 |
| `part-*` | 12 | 7 |
| `bs-*` | 12 | 5 |
| `e24-*` | 12 | 1 |
| `don-*` | 11 | 1 |
| `esp-*` | 11 | 0 |
| `vote-*` | 11 | 0 |
| `td-*` | 9 | 4 |
| `att-*` | 7 | 34 |
| `int-*` | 7 | 15 |
| `site-*` | 5 | 0 |
| `signal-*` | 4 | 1 |
| `stat-*` | 4 | 0 |
| `pp-*` | 4 | 0 |
| `hou-*` | 4 | 0 |
| `lob-*` | 3 | 47 |
| `section-*` | 2 | 1 |
| `sidebar-*` | 1 | 0 |
| `is-*` | 1 | 9 |
| `sr-*` | 1 | 0 |
| `body-*` | 1 | 0 |

## Per-module vocabulary

| Module | Classes emitted |
|---|---:|
| `utility/pages_code/member_overview.py` | 117 |
| `utility/ui/components.py` | 116 |
| `utility/pages_code/corporate.py` | 106 |
| `utility/pages_code/judiciary.py` | 106 |
| `utility/pages_code/public_appointments.py` | 69 |
| `utility/pages_code/election_2024.py` | 51 |
| `utility/pages_code/legislation.py` | 51 |
| `utility/ui/vote_explorer.py` | 51 |
| `utility/pages_code/statutory_instruments.py` | 49 |
| `utility/pages_code/constituency.py` | 45 |
| `utility/pages_code/your_council.py` | 45 |
| `utility/pages_code/local_government.py` | 39 |
| `utility/pages_code/procurement/patterns.py` | 35 |
| `utility/pages_code/public_payments.py` | 34 |
| `utility/pages_code/follow_the_money.py` | 31 |
| `utility/pages_code/committees.py` | 29 |
| `utility/pages_code/lobbying_3.py` | 26 |
| `utility/pages_code/procurement/ted.py` | 25 |
| `utility/pages_code/procurement/pay_profiles.py` | 24 |
| `utility/pages_code/procurement/councils.py` | 19 |
| `utility/pages_code/ministerial_diaries.py` | 17 |
| `utility/pages_code/procurement/_shared.py` | 17 |
| `utility/pages_code/procurement/national.py` | 15 |
| `utility/ui/payments_panel.py` | 13 |
| `utility/pages_code/attendance.py` | 12 |
| `utility/pages_code/company.py` | 12 |
| `utility/pages_code/procurement/payments.py` | 12 |
| `utility/pages_code/procurement/profiles.py` | 12 |
| `utility/pages_code/votes.py` | 12 |
| `utility/ui/attendance_panel.py` | 12 |
| `utility/pages_code/procurement/tenders.py` | 10 |
| `utility/pages_code/glossary.py` | 8 |
| `utility/pages_code/body.py` | 5 |
| `utility/pages_code/payments.py` | 5 |
| `utility/pages_code/procurement/browse.py` | 5 |
| `utility/shared_css.py` | 5 |
| `utility/ui/interests_panel.py` | 5 |
| `utility/pages_code/accommodation_spend.py` | 4 |
| `utility/pages_code/housing.py` | 4 |
| `utility/pages_code/procurement/page.py` | 3 |
| `utility/ui/entity_links.py` | 3 |
| `utility/pages_code/your_councillors.py` | 2 |
| `utility/ui/data_integrity_panel.py` | 1 |

## Migration rule

Components may add classes freely. A class in the contract may only be renamed if the CSS is renamed with it in the same change. Treat this table as the acceptance test for visual parity: same names + same stylesheet = same design.

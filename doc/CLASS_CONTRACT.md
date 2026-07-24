# CSS class contract — the styling vocabulary

> **GENERATED — do not hand-edit.** Regenerate with `python tools/migration/extract_class_contract.py -o doc/CLASS_CONTRACT.md`.

> Verify with `python tools/migration/extract_class_contract.py --check`.

Styling is preserved across a framework change if the new components emit these class names against this stylesheet. That makes the vocabulary below a contract — the same kind as [URL_CONTRACT.md](URL_CONTRACT.md), and equally unwritten until now.

## Summary

| Measure | Count |
|---|---:|
| Class names emitted by the UI | 993 |
| Class selectors defined in CSS | 1,326 |
| **The contract** (emitted AND styled) | **977** |
| Dynamic stems (f-string built, e.g. `pill-{kind}`) | 2 |
| Dead CSS (styled, never emitted) | 321 |
| Unstyled (emitted, never styled) | 16 |

## Contract by family

A React component reproducing one of these families must emit the same names.

| Family | In contract | Dead |
|---|---:|---:|
| `dt-*` | 105 | 18 |
| `corp-*` | 103 | 12 |
| `pr-*` | 71 | 19 |
| `jd-*` | 70 | 3 |
| `mo-*` | 54 | 5 |
| `si-*` | 48 | 9 |
| `leg-*` | 45 | 9 |
| `con-*` | 43 | 2 |
| `pa-*` | 39 | 5 |
| `(no prefix)` | 30 | 49 |
| `q-*` | 29 | 3 |
| `jud-*` | 28 | 3 |
| `cmt-*` | 25 | 6 |
| `lp3-*` | 25 | 2 |
| `sb-*` | 23 | 1 |
| `mf-*` | 22 | 1 |
| `vt-*` | 22 | 8 |
| `sc-*` | 19 | 3 |
| `yc-*` | 17 | 0 |
| `lg-*` | 16 | 3 |
| `pay-*` | 16 | 23 |
| `bs-*` | 12 | 5 |
| `part-*` | 12 | 8 |
| `e24-*` | 12 | 2 |
| `vote-*` | 11 | 0 |
| `don-*` | 11 | 1 |
| `esp-*` | 11 | 0 |
| `td-*` | 9 | 5 |
| `int-*` | 7 | 16 |
| `att-*` | 7 | 35 |
| `sa-*` | 5 | 0 |
| `site-*` | 5 | 1 |
| `hou-*` | 4 | 1 |
| `signal-*` | 4 | 1 |
| `stat-*` | 4 | 0 |
| `pp-*` | 4 | 1 |
| `lob-*` | 3 | 48 |
| `section-*` | 2 | 2 |
| `sr-*` | 1 | 0 |
| `body-*` | 1 | 0 |
| `is-*` | 1 | 9 |
| `sidebar-*` | 1 | 0 |

## Per-module vocabulary

| Module | Classes emitted |
|---|---:|
| `utility/pages_code/member_overview.py` | 117 |
| `utility/pages_code/corporate.py` | 106 |
| `utility/pages_code/judiciary.py` | 106 |
| `utility/pages_code/procurement.py` | 90 |
| `utility/ui/components.py` | 90 |
| `utility/pages_code/public_appointments.py` | 69 |
| `utility/pages_code/election_2024.py` | 51 |
| `utility/pages_code/legislation.py` | 51 |
| `utility/ui/vote_explorer.py` | 51 |
| `utility/pages_code/statutory_instruments.py` | 49 |
| `utility/pages_code/constituency.py` | 45 |
| `utility/pages_code/local_government.py` | 39 |
| `utility/pages_code/public_payments.py` | 34 |
| `utility/pages_code/your_council.py` | 33 |
| `utility/pages_code/follow_the_money.py` | 30 |
| `utility/pages_code/committees.py` | 26 |
| `utility/pages_code/lobbying_3.py` | 26 |
| `utility/pages_code/siting_check.py` | 19 |
| `utility/pages_code/ministerial_diaries.py` | 17 |
| `utility/ui/payments_panel.py` | 13 |
| `utility/pages_code/attendance.py` | 12 |
| `utility/pages_code/company.py` | 12 |
| `utility/pages_code/votes.py` | 12 |
| `utility/ui/attendance_panel.py` | 12 |
| `utility/pages_code/glossary.py` | 8 |
| `utility/pages_code/payments.py` | 5 |
| `utility/pages_code/siting_assistant.py` | 5 |
| `utility/shared_css.py` | 5 |
| `utility/ui/interests_panel.py` | 5 |
| `utility/pages_code/housing.py` | 4 |
| `utility/ui/entity_links.py` | 3 |
| `utility/pages_code/accommodation_spend.py` | 2 |
| `utility/pages_code/your_councillors.py` | 2 |

## Migration rule

Components may add classes freely. A class in the contract may only be renamed if the CSS is renamed with it in the same change. Treat this table as the acceptance test for visual parity: same names + same stylesheet = same design.

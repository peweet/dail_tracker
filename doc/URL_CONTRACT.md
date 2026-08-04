# URL contract — the deep-link spec

> **GENERATED — do not hand-edit.** Regenerate with `python tools/migration/extract_url_contract.py -o doc/URL_CONTRACT.md`.

> Verify with `python tools/migration/extract_url_contract.py --check` (fails on drift).

This is the one part of the UI that outside parties depend on: bookmarks, shared links, search results, and any future React router. Streamlit widgets and CSS can be redesigned freely; **these strings cannot** without breaking links already in the wild.

## Routes (30)

| Route (`url_path`) | Title | Page module | app.py line |
|---|---|---|---:|
| `?page=accommodation-spend` | Accommodation Spend | `pages_code.accommodation_spend` | 254 |
| `?page=body` | Public bodies | `pages_code.body` | 279 |
| `?page=company` | Companies | `pages_code.company` | 269 |
| `?page=constituencies` | Constituencies | `pages_code.constituency` | 125 |
| `?page=follow-the-money` | Follow the Money | `pages_code.follow_the_money` | 247 |
| `?page=glossary` | Glossary | `pages_code.glossary` | 328 |
| `?page=home` | Home | `_home_page` | 181 |
| `?page=housing` | Housing | `pages_code.housing` | 173 |
| `?page=local-government` | Who Runs Your County | `pages_code.local_government` | 146 |
| `?page=member-overview` | Member Overview | `pages_code.member_overview` | 188 |
| `?page=rankings-appointments` | Appointments | `pages_code.public_appointments` | 320 |
| `?page=rankings-attendance` | Attendance | `pages_code.attendance` | 194 |
| `?page=rankings-committees` | Committees | `pages_code.committees` | 213 |
| `?page=rankings-corporate` | Corporate Notices | `pages_code.corporate` | 299 |
| `?page=rankings-council-spending` | Council Spending | `pages_code.council_spending` | 166 |
| `?page=rankings-election-spending` | Election Finance | `pages_code.election_2024` | 228 |
| `?page=rankings-interests` | Interests | `_interests_redirect_page` | 207 |
| `?page=rankings-judiciary` | Courts & Judiciary | `pages_code.judiciary` | 305 |
| `?page=rankings-legislation` | Legislation | `pages_code.legislation` | 287 |
| `?page=rankings-lobbying` | Lobbying | `pages_code.lobbying_3` | 313 |
| `?page=rankings-ministerial-diaries` | Who Ministers Meet | `pages_code.ministerial_diaries` | 314 |
| `?page=rankings-payments` | TD Payments | `pages_code.payments` | 224 |
| `?page=rankings-procurement` | Procurement | `pages_code.procurement` | 234 |
| `?page=rankings-public-payments` | Public Payments | `pages_code.public_payments` | 260 |
| `?page=rankings-statutory-instruments` | Statutory Instruments | `pages_code.statutory_instruments` | 293 |
| `?page=rankings-votes` | Votes | `pages_code.votes` | 200 |
| `?page=support` | Support | `pages_code.support` | 333 |
| `?page=what-they-own` | What They Own | `pages_code.what_they_own` | 113 |
| `?page=your-council` | Your Council | `pages_code.your_council` | 135 |
| `?page=your-councillors` | Your Councillors | `pages_code.your_councillors` | 156 |

## Query parameters (58 distinct)

Every literal key read from or written to `st.query_params`, with the modules that use it. A key used by more than one module is a **shared contract** — changing it breaks every listed consumer.

| Parameter | Modules | Shared? |
|---|---|---|
| `att_td` | `utility/pages_code/attendance.py` | no |
| `authority` | `utility/pages_code/procurement/page.py` | no |
| `bill` | `utility/pages_code/legislation.py` | no |
| `buyer` | `utility/pages_code/body.py` | no |
| `cand` | `utility/pages_code/election_2024.py` | no |
| `category` | `utility/pages_code/public_payments.py` | no |
| `clear` | `utility/pages_code/corporate.py`<br>`utility/pages_code/public_appointments.py`<br>`utility/pages_code/statutory_instruments.py` | **yes** |
| `clr_county` | `utility/pages_code/your_councillors.py` | no |
| `clr_lea` | `utility/pages_code/your_councillors.py` | no |
| `clr_name` | `utility/pages_code/your_councillors.py` | no |
| `committee` | `utility/pages_code/committees.py` | no |
| `constituency` | `utility/pages_code/constituency.py` | no |
| `council` | `utility/pages_code/constituency.py`<br>`utility/pages_code/your_council.py` | **yes** |
| `county` | `utility/pages_code/housing.py` | no |
| `court` | `utility/pages_code/judiciary.py` | no |
| `cpv` | `utility/pages_code/procurement/page.py` | no |
| `dept` | `utility/pages_code/ministerial_diaries.py` | no |
| `dparty` | `utility/pages_code/election_2024.py` | no |
| `eparty` | `utility/pages_code/election_2024.py` | no |
| `firm` | `utility/pages_code/corporate.py` | no |
| `flow_group` | `utility/pages_code/follow_the_money.py` | no |
| `flow_supplier_lines` | `utility/pages_code/follow_the_money.py` | no |
| `from` | `utility/pages_code/votes.py` | no |
| `fund` | `utility/pages_code/corporate.py` | no |
| `gparty` | `utility/pages_code/election_2024.py` | no |
| `judge` | `utility/pages_code/judiciary.py` | no |
| `la` | `utility/pages_code/local_government.py` | no |
| `lp3_area` | `utility/pages_code/lobbying_3.py` | no |
| `lp3_dpo` | `utility/pages_code/lobbying_3.py` | no |
| `lp3_org` | `utility/pages_code/lobbying_3.py` | no |
| `lp3_orgindex` | `utility/pages_code/lobbying_3.py` | no |
| `lp3_pol` | `utility/pages_code/lobbying_3.py` | no |
| `lp3_rd` | `utility/pages_code/lobbying_3.py` | no |
| `lp3_result_pol` | `utility/pages_code/lobbying_3.py` | no |
| `lp3_topic` | `utility/pages_code/lobbying_3.py` | no |
| `member` | `utility/pages_code/attendance.py`<br>`utility/pages_code/committees.py`<br>`utility/pages_code/member_overview.py`<br>`utility/pages_code/payments.py`<br>`utility/pages_code/votes.py` | **yes** |
| `minister` | `utility/pages_code/ministerial_diaries.py` | no |
| `mo_q_topic` | `utility/pages_code/member_overview.py` | no |
| `org` | `utility/pages_code/ministerial_diaries.py` | no |
| `paid_publisher` | `utility/pages_code/council_spending.py`<br>`utility/pages_code/follow_the_money.py`<br>`utility/pages_code/procurement/page.py`<br>`utility/pages_code/your_council.py` | **yes** |
| `paid_supplier` | `utility/pages_code/council_spending.py`<br>`utility/pages_code/follow_the_money.py`<br>`utility/pages_code/procurement/page.py`<br>`utility/pages_code/procurement/pay_profiles.py`<br>`utility/pages_code/your_council.py` | **yes** |
| `paid_tier` | `utility/pages_code/council_spending.py`<br>`utility/pages_code/follow_the_money.py`<br>`utility/pages_code/procurement/page.py`<br>`utility/pages_code/procurement/pay_profiles.py` | **yes** |
| `payyr` | `utility/ui/payments_panel.py` | no |
| `pp` | `utility/pages_code/public_payments.py` | no |
| `publisher` | `utility/pages_code/public_payments.py` | no |
| `q` | `utility/pages_code/corporate.py` | no |
| `ref` | `utility/pages_code/corporate.py`<br>`utility/pages_code/public_appointments.py` | **yes** |
| `reg` | `utility/pages_code/procurement/_shared.py` | no |
| `section` | `utility/pages_code/member_overview.py` | no |
| `si` | `utility/pages_code/statutory_instruments.py` | no |
| `single_bid_cpv` | `utility/pages_code/procurement/page.py` | no |
| `spark` | `utility/pages_code/corporate.py`<br>`utility/pages_code/public_appointments.py` | **yes** |
| `supplier` | `utility/pages_code/company.py`<br>`utility/pages_code/procurement/page.py`<br>`utility/pages_code/public_payments.py` | **yes** |
| `tab` | `utility/pages_code/housing.py`<br>`utility/pages_code/procurement/_shared.py` | **yes** |
| `ted_winner` | `utility/pages_code/procurement/page.py` | no |
| `view` | `utility/pages_code/election_2024.py` | no |
| `vote` | `utility/pages_code/votes.py` | no |
| `yc` | `utility/pages_code/your_council.py` | no |

## Link emitters

Modules that BUILD urls (as opposed to reading them). These are what put links into the wild, so they define what must keep working.

| Module | Keys emitted |
|---|---|
| `utility/pages_code/committees.py` | `committee` |
| `utility/pages_code/company.py` | `flow_supplier_lines`, `paid_tier`, `supplier` |
| `utility/pages_code/constituency.py` | `constituency`, `council` |
| `utility/pages_code/corporate.py` | `clear`, `firm`, `fund`, `q`, `ref`, `spark` |
| `utility/pages_code/election_2024.py` | `cand`, `dparty`, `eparty`, `gparty`, `view` |
| `utility/pages_code/housing.py` | `tab` |
| `utility/pages_code/judiciary.py` | `court`, `judge` |
| `utility/pages_code/lobbying_3.py` | `lp3_area`, `lp3_dpo`, `lp3_org`, `lp3_orgindex`, `lp3_pol`, `lp3_result_pol`, `lp3_topic` |
| `utility/pages_code/local_government.py` | `la` |
| `utility/pages_code/member_overview.py` | `member`, `mo_q_topic`, `section` |
| `utility/pages_code/ministerial_diaries.py` | `dept`, `minister`, `org` |
| `utility/pages_code/procurement/_shared.py` | `authority`, `cpv`, `paid_publisher`, `paid_supplier`, `paid_tier`, `single_bid_cpv` |
| `utility/pages_code/procurement/payments.py` | `paid_publisher`, `paid_tier` |
| `utility/pages_code/public_appointments.py` | `clear`, `ref`, `spark` |
| `utility/pages_code/public_payments.py` | `category`, `pp`, `publisher`, `supplier` |
| `utility/pages_code/statutory_instruments.py` | `clear`, `si` |
| `utility/pages_code/your_council.py` | `clr_county`, `clr_name`, `council`, `yc` |
| `utility/ui/components.py` | `body`, `from`, `labels`, `title` |
| `utility/ui/entity_links.py` | `authority`, `bill`, `buyer`, `la`, `lp3_org`, `member`, `paid_publisher`, `paid_tier`, `q`, `reg`, `section`, `si`, `supplier`, `tab`, `ted_winner`, `vote` |
| `utility/ui/payments_panel.py` | `payyr` |

## Migration rule

A React router must accept every route and parameter above **unchanged**. New parameters may be added; existing ones may only be removed behind a redirect that preserves the old link. Treat this table as the acceptance test for routing parity.

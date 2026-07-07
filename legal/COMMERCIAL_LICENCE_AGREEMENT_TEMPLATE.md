# DÁIL TRACKER — COMMERCIAL SOFTWARE LICENCE AGREEMENT (TEMPLATE)

> **DRAFT TEMPLATE — NOT YET LEGALLY REVIEWED.** This is a starting point for a
> commercial licence and **must be reviewed by a qualified Irish solicitor
> (software IP)** before it is offered to or signed with any customer. Bracketed
> `{{PLACEHOLDERS}}` and `[OPTION]` blocks must be completed/selected per deal.
> This document is **not legal advice.**

---

This Commercial Software Licence Agreement (the **"Agreement"**) is made on
`{{EFFECTIVE_DATE}}` between:

- **Licensor:** Patrick Glynn, of {{LICENSOR_ADDRESS}} (the **"Licensor"**); and
- **Licensee:** `{{LICENSEE_NAME}}`, of `{{LICENSEE_ADDRESS}}` (the **"Licensee"**).

The Licensor and Licensee are each a **"Party"** and together the **"Parties"**.

## Background

(A) The Licensor is the author and copyright owner of the software project known
as **"Dáil Tracker"** (the **"Software"**), which the Licensor also makes
available to the public under the GNU Affero General Public License v3.0 (the
**"AGPL"**).

(B) The Licensee wishes to use the Software on terms that are not subject to the
copyleft and network-source-disclosure obligations of the AGPL, and the Licensor
is willing to grant such a licence on the terms of this Agreement.

The Parties agree as follows.

---

## 1. Definitions

- **"Software"** — the source code and object code of Dáil Tracker authored by
  the Licensor: the ETL pipeline, parsers, SQL views, the `dail_tracker_core`
  library, the JSON API, and the Streamlit application, together with any Updates
  the Licensor supplies under this Agreement. The Software **excludes** the Data
  (clause 2.3) and any third-party components (clause 7).
- **"Data"** — any dataset, derived data file, or extracted content processed,
  bundled with, or produced by the Software that originates from a third party or
  public body (including the Houses of the Oireachtas, Iris Oifigiúil, the
  Companies Registration Office, the Charities Regulator, SIPO, the CSO, and the
  Courts Service).
- **"Licensee Product"** — the Licensee's product or service that incorporates,
  embeds, or is built using the Software.
- **"Updates"** — bug fixes, patches, and new versions of the Software the
  Licensor elects to provide under clause 4.
- **"Fees"** — the amounts payable under clause 5 and Schedule 1.

## 2. Licence grant

2.1 **Grant.** Subject to the Licensee's payment of the Fees and compliance with
this Agreement, the Licensor grants the Licensee a **non-exclusive,
non-transferable, non-sub-licensable** (except as in clause 2.4) licence, for the
Term, to:

  (a) use, reproduce, and modify the Software;
  (b) incorporate and embed the Software into the Licensee Product; and
  (c) host, distribute, and otherwise commercially exploit the Licensee Product,

**without** being subject to sections 5, 6, or 13 of the AGPL or any other
copyleft/source-disclosure obligation of the AGPL with respect to the Software.

2.2 **Relationship to the AGPL.** This Agreement is an **alternative** to the
AGPL for the Licensee. Where the Licensee operates under this Agreement, the AGPL
does not apply to the Licensee's use of the Software. The Software remains
available to the public under the AGPL independently of this Agreement.

2.3 **Data excluded.** This Agreement grants **no** rights of any kind in the
Data. The Data remains subject to the licences and terms of its respective
sources, as described in the project's `NOTICE.md`. The Licensee is solely
responsible for obtaining any rights it needs to use the Data and for complying
with each source's licence, attribution, and re-use terms — including, without
limitation, that **Iris Oifigiúil** material is Government of Ireland copyright
and is **not** licensed to the Licensee hereunder.

2.4 **Affiliates / sub-licensing.** [OPTION — select one]
  - **(default)** The licence may not be sub-licensed or extended to affiliates
    without the Licensor's prior written consent; or
  - The licence extends to the Licensee's wholly-owned affiliates listed in
    Schedule 1, and the Licensee may grant end users of the Licensee Product a
    sub-licence to use the Software solely as embedded in the Licensee Product.

2.5 **Reservation.** All rights not expressly granted are reserved by the
Licensor. The Licensor retains all right, title, and interest in the Software.

## 3. Restrictions

The Licensee shall not:

  (a) use the Software except as permitted by this Agreement;
  (b) remove or obscure the Licensor's copyright notices in the Software's source
      (notices need not be exposed in the Licensee Product's UI);
  (c) use the Licensor's name, the "Dáil Tracker" name, or any Licensor
      trademark except as permitted in writing (see the project Trademark Policy);
  (d) represent that the Licensee Product is endorsed by or affiliated with the
      Licensor without written agreement; or
  (e) assert that the AGPL public version infringes any Licensee rights arising
      from this Agreement.

## 4. Updates and support

4.1 [OPTION] The Licensor [shall / shall use reasonable efforts to / is under no
obligation to] provide Updates during the Term.

4.2 [OPTION] Support, if any, is provided at the level set out in Schedule 1
(e.g. email support, response targets). Absent a Schedule 1 entry, the Software
is licensed **as-is** with no support obligation.

## 5. Fees

5.1 The Licensee shall pay the Fees set out in **Schedule 1**. [OPTION — choose a
model; see Schedule 1.]

5.2 Fees are exclusive of VAT and other taxes, which the Licensee shall pay where
applicable.

5.3 Invoices are payable within `{{PAYMENT_DAYS}}` days. Overdue amounts accrue
interest under the European Communities (Late Payment in Commercial Transactions)
Regulations.

5.4 **Royalty reporting & audit** *(applies if a revenue-share model is chosen).*
The Licensee shall keep accurate records of revenue relevant to the royalty and
provide a statement each `{{REPORTING_PERIOD}}`. The Licensor may, on reasonable
notice and not more than once per year, audit those records solely to verify the
Fees; if an audit reveals an underpayment exceeding 5%, the Licensee shall bear
the reasonable cost of the audit.

## 6. Warranties and liability

6.1 Each Party warrants it has the authority to enter this Agreement.

6.2 The Licensor warrants that, to its knowledge, it owns or controls the
copyright in the Software necessary to grant this licence. **Except as expressly
stated, the Software is provided "AS IS"** and the Licensor disclaims all other
warranties (including merchantability, fitness for purpose, and
non-infringement) to the maximum extent permitted by law. The Licensor gives **no
warranty as to the Data** (clause 2.3) or the accuracy, completeness, or
currency of any output.

6.3 **Liability cap.** Subject to clause 6.4, each Party's total aggregate
liability arising out of or in connection with this Agreement shall not exceed
`{{LIABILITY_CAP}}` (e.g. the total Fees paid in the 12 months preceding the
claim).

6.4 Nothing limits liability for death or personal injury caused by negligence,
fraud, or any liability that cannot lawfully be limited.

6.5 Neither Party is liable for indirect or consequential loss, loss of profit,
or loss of data.

## 7. Third-party components

The Software depends on open-source third-party libraries listed in the project's
dependency manifest, each under its own licence. Those libraries are licensed to
the Licensee by their respective authors, not by the Licensor, and this Agreement
does not alter their terms.

## 8. Term and termination

8.1 This Agreement runs for `{{TERM}}` from the Effective Date and [renews
automatically for successive periods / expires] unless terminated.

8.2 Either Party may terminate for the other's material breach not remedied
within 30 days of written notice.

8.3 On termination for the Licensee's breach or non-payment, the licence in
clause 2 ends and the Licensee must cease using the Software under this Agreement
(the Licensee may thereafter use the public version only under the AGPL).

8.4 [OPTION] **Survival of distributed copies.** Licences validly granted to end
users for copies of the Licensee Product distributed before termination survive,
provided the Licensee is not in payment default.

8.5 Clauses 2.3, 2.5, 6, 9, and 10 survive termination.

## 9. Governing law and disputes

9.1 This Agreement is governed by the **laws of Ireland**.

9.2 The Parties submit to the **exclusive jurisdiction of the courts of Ireland**.

## 10. General

10.1 **Entire agreement.** This Agreement (with its Schedules) is the entire
agreement on its subject matter and supersedes prior discussions.

10.2 **Assignment.** The Licensee may not assign without the Licensor's written
consent (not to be unreasonably withheld); the Licensor may assign to a successor
of the project (e.g. on incorporation).

10.3 **Notices.** Notices go to the addresses above (or `{{LICENSING_EMAIL}}` for
the Licensor) and are deemed received on delivery.

10.4 **No partnership.** Nothing creates a partnership, agency, or employment.

10.5 **Variation.** Only in writing signed by both Parties.

---

## Schedule 1 — Commercial terms

| Item | Value |
|---|---|
| Licensed Software version / scope | `{{SCOPE}}` |
| Permitted use | `{{PERMITTED_USE}}` (e.g. one SaaS product, internal only, OEM redistribution) |
| Affiliates (clause 2.4) | `{{AFFILIATES}}` |
| **Fee model** | `[ ] Annual subscription  [ ] Revenue-share royalty  [ ] Per-deployment` |
| Annual subscription fee | `{{ANNUAL_FEE}}` per year |
| Royalty rate | `{{ROYALTY_PCT}}` % of `{{ROYALTY_BASE}}` (defined revenue) |
| Per-deployment / per-seat fee | `{{UNIT_FEE}}` per `{{UNIT}}` |
| Reporting period (royalty) | `{{REPORTING_PERIOD}}` |
| Payment terms | `{{PAYMENT_DAYS}}` days |
| Term | `{{TERM}}` |
| Support level | `{{SUPPORT_LEVEL}}` |
| Liability cap | `{{LIABILITY_CAP}}` |

---

**Signed for and on behalf of the Licensor:**

Name: Patrick Glynn  Signature: __________________  Date: __________

**Signed for and on behalf of the Licensee:**

Name: ____________  Title: __________  Signature: __________  Date: __________

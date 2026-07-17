---
tier: PLAN
status: LIVE
domain: commercial
updated: 2026-06-28
supersedes: []
read_when: setting up, reviewing, or invoking the AGPL/commercial dual-licensing scaffolding (CLA, COMMERCIAL-LICENSE.md) before a first commercial sale
key: PLAN|LIVE|commercial
---

# Commercialisation & Dual-Licensing Plan — Dáil Tracker

**Status:** scaffolding created 2026-06-28; **legal review and placeholder fill-in still required before first commercial use.**
**Owner:** Patrick Glynn.
**Goal:** keep Dáil Tracker genuinely open source *and* be paid (royalties/fees) when a business uses it commercially.

> Nothing in this document or the files it references is legal advice. The
> commercial agreement and CLA are **drafts/templates** — have an Irish
> solicitor who does software IP review them before you sign or rely on them.

---

## 1. Strategy in one paragraph

We use **dual licensing**. The code stays published under **AGPL-3.0** (already in
[`LICENSE`](../LICENSE)). AGPL is the strongest copyleft: anyone who hosts a
*modified* version as a network service must publish their modified source under
AGPL too. For most commercial SaaS operators that is a dealbreaker — so they buy
a **separate commercial licence** from us that waives the AGPL source-disclosure
obligation, in exchange for a fee/royalty. The AGPL is the lever; the commercial
licence is the revenue. Everyone who is happy with AGPL terms keeps using it for
free. This is the MySQL / Qt / GitLab model and is the *only* route that is both
truly OSI open source and royalty-bearing for commercial users.

Three things make it actually work, and all three are set up by this plan:

1. **We must own/control all copyright.** The moment we merge an outside
   contribution under plain AGPL, we can no longer relicense *that* contribution
   commercially. Fixed by the **CLA** ([`CLA.md`](../CLA.md)) — every contributor
   grants us the right to relicense their contribution under any terms, including
   commercial.
2. **A commercial offer must exist and be discoverable** so buyers know it's an
   option. Fixed by [`COMMERCIAL-LICENSE.md`](../COMMERCIAL-LICENSE.md) (the
   explainer) + the agreement template + the website page.
3. **The brand is a second, independent lever.** A **trademark** on "Dáil
   Tracker" lets us control commercial use of the *name* even where copyleft
   doesn't reach. See [`legal/TRADEMARK_POLICY.md`](../legal/TRADEMARK_POLICY.md).

**Hard limit (do not forget):** all of this monetises **our code, schema,
extractors and curation** only. The **data** is third-party public-sector
material under its own licences (Oireachtas CC-BY, Iris Government copyright,
etc. — see [`NOTICE.md`](../NOTICE.md)). We cannot charge royalties on the data
or sub-licence it. A commercial licensee must still comply with each data
source's licence independently. Price and pitch the commercial licence on the
**software and pipeline**, not the datasets.

---

## 2. What was created (file inventory)

| File | Purpose | Audience |
|---|---|---|
| [`LICENSE`](../LICENSE) | AGPL-3.0 full text (pre-existing) | Everyone (open-source users) |
| [`COMMERCIAL-LICENSE.md`](../COMMERCIAL-LICENSE.md) | Plain-language: *when do you need a commercial licence, what it grants, how to buy* | Prospective commercial buyers |
| [`legal/COMMERCIAL_LICENCE_AGREEMENT_TEMPLATE.md`](../legal/COMMERCIAL_LICENCE_AGREEMENT_TEMPLATE.md) | The actual signable contract (draft template, royalty options) | Buyer + your solicitor |
| [`CLA.md`](../CLA.md) | Contributor Licence Agreement — preserves your right to dual-licence | Contributors |
| [`CONTRIBUTING.md`](../CONTRIBUTING.md) | How to contribute; points at the CLA | Contributors |
| [`legal/TRADEMARK_POLICY.md`](../legal/TRADEMARK_POLICY.md) | Acceptable use of the "Dáil Tracker" name/brand | Everyone |
| [`legal/website_licensing_page.md`](../legal/website_licensing_page.md) | Drop-in content for the `/licensing` page on your domain | Web |
| `NOTICE.md`, `README.MD` | Updated to state dual licensing | Everyone |

---

## 3. ACTION REQUIRED (human-only — do these when you're back)

Ordered by urgency. Items 1–3 are needed before you accept any contribution or
quote any customer; 4–7 can follow.

### 3.1 — Fill in the placeholders (5 minutes)
Every template uses `{{DOUBLE_BRACE}}` tokens for things only you can supply.
Find them all:

```powershell
# from repo root — list every placeholder occurrence
Get-ChildItem -Recurse -Include *.md -Path .\COMMERCIAL-LICENSE.md,.\CLA.md,.\CONTRIBUTING.md,.\legal,.\doc\COMMERCIALISATION_PLAN.md |
  Select-String -Pattern '{{[A-Z_]+}}' | Select-Object Path, LineNumber, Line
```

Then replace them in one pass (edit the values first):

```powershell
$repl = @{
  '{{DOMAIN}}'           = 'dailtracker.ie'                 # <-- your registered domain
  '{{LICENSING_EMAIL}}'  = 'licensing@dailtracker.ie'       # <-- mailbox you will monitor
  '{{LICENSOR_ADDRESS}}' = '<your postal address for legal notices>'
}
$files = Get-ChildItem -Recurse -Include *.md -Path .\COMMERCIAL-LICENSE.md,.\CLA.md,.\CONTRIBUTING.md,.\legal
foreach ($f in $files) {
  $t = Get-Content $f.FullName -Raw
  foreach ($k in $repl.Keys) { $t = $t.Replace($k, $repl[$k]) }
  Set-Content $f.FullName $t -Encoding utf8
}
```

Placeholder reference:
- `{{DOMAIN}}` — the registered domain (you said it's ready to go).
- `{{LICENSING_EMAIL}}` — the inbox for commercial enquiries (e.g. `licensing@…`).
- `{{LICENSOR_ADDRESS}}` — postal address for contractual notices.
- Licensor name is hard-coded to **Patrick Glynn** (sole trader). If you
  incorporate, search/replace to the company name + CRO number and re-assign the
  copyright to the company (a one-line IP-assignment your solicitor can do).

### 3.2 — Engage a solicitor (the one thing you can't DIY)
Have an **Irish software-IP solicitor** review, before first use:
- [`legal/COMMERCIAL_LICENCE_AGREEMENT_TEMPLATE.md`](../legal/COMMERCIAL_LICENCE_AGREEMENT_TEMPLATE.md) — the contract you'll actually sign with customers. Key clauses to confirm: royalty/fee mechanics, liability cap, the data-exclusion clause, audit rights, governing law (Ireland).
- [`CLA.md`](../CLA.md) — confirm the relicensing grant is enforceable and that the inbound grant is broad enough to support commercial relicensing.
This is a one-time cost producing reusable templates; budget a few hours of their time.

### 3.3 — Turn on CLA enforcement before merging any external PR
Until a contributor has signed the CLA, **do not merge their code** — it
poisons your ability to dual-licence.
- **Already scaffolded:** [`.github/workflows/cla.yml`](../.github/workflows/cla.yml)
  (CLA Assistant Lite). It activates automatically once merged to `main` — no
  GitHub app install or secret needed (same-repo storage uses the built-in
  `GITHUB_TOKEN`; it records signatures on an auto-created `cla-signatures`
  branch). Dormant while you're the sole author; it bites the first time someone
  else opens a PR.
- Note: **[DCO](https://developercertificate.org/) alone is not sufficient** for
  dual-licensing (it certifies provenance, not relicensing rights). The CLA's
  relicensing grant is what matters.

### 3.4 — Decide the pricing / royalty model
The agreement template ships with three interchangeable fee structures (annual
subscription, revenue-share royalty, per-deployment). Pick a default and a
rough number. Guidance in [`COMMERCIAL-LICENSE.md`](../COMMERCIAL-LICENSE.md) §"Pricing".
You can decide this last — you don't need it to publish the open-source side.

### 3.5 — Wire up the domain
- Create the `{{LICENSING_EMAIL}}` mailbox.
- Publish [`legal/website_licensing_page.md`](../legal/website_licensing_page.md) at `https://{{DOMAIN}}/licensing`.
- Point `README.MD` and `COMMERCIAL-LICENSE.md` enquiry links at it (already templated).

### 3.6 — File the trademark
- **Turnkey runbook:** [`legal/TRADEMARK_FILING_CHECKLIST.md`](../legal/TRADEMARK_FILING_CHECKLIST.md)
  — scope decision, Nice classes (9 + 42 core), pre-filing clearance search,
  IPOI filing steps, and the post-filing timeline, as a tick-box list.
- Short version: file a national word-mark with the **IPOI** (<https://www.ipoi.gov.ie/>)
  in **Class 9 + 42**; clearance-search first; **"Dáil" is a government term** so
  get a TM attorney's distinctiveness read (a logo mark is the fallback).
- **You can use `™` immediately** — no registration needed; `®` only after grant.
- See [`legal/TRADEMARK_POLICY.md`](../legal/TRADEMARK_POLICY.md) for the usage policy that backs the mark.

### 3.7 — Copyright registration (low priority)
Copyright is **automatic** in Ireland/EU — no registration needed or available
for enforceability. If you ever expect **US** infringement litigation, US
Copyright Office registration adds statutory-damages eligibility there. Not
urgent; note and move on.

### 3.8 — Optional: SPDX headers in source files
Per-file licence headers make provenance unambiguous and help automated
licence scanners. **Not done automatically** (it's a large, noisy diff across
hundreds of files and is better reviewed deliberately). When you want it, the
header is:

```python
# SPDX-License-Identifier: AGPL-3.0-or-later
# SPDX-FileCopyrightText: © 2026 Patrick Glynn
# Dual-licensed: commercial licence available — see COMMERCIAL-LICENSE.md
```

A script to apply it to first-party Python (excluding `.venv`, sandbox, vendored
trees) can be added on request; keep it a separate, reviewable commit.

---

## 4. Enforcement & operating notes

- **AGPL enforcement is on you to detect.** The licence is self-executing but
  nobody polices it for you. In practice the *risk-aversion of companies' own
  legal teams* drives most commercial-licence sales — they don't want the
  source-disclosure exposure. Periodically search for hosted clones/forks.
- **Keep clean provenance.** Sole authorship today = you own 100%. Protect it:
  CLA before external contributions, copyright headers, a `CONTRIBUTORS`/AUTHORS
  record over time.
- **Don't accept AGPL-incompatible third-party code** into the codebase that you
  then can't relicense — it would break the commercial grant. The CLA only
  covers *contributions to us*, not third-party libraries you vendor.
- **Two-grain offer:** a customer either (a) complies with AGPL for free, or
  (b) pays for the commercial licence. There is no third "free commercial" tier
  — that would undercut the model.
- **Data stays excluded** from every commercial deal (clause in the template).
  Never imply you can licence the Oireachtas/Iris/CRO data to a customer.

---

## 5. Sequencing (suggested)

```
Now (autonomous)         3.1 fill placeholders ──► 3.5 wire domain ──► publish open-source side as-is
Before 1st contributor   3.3 CLA enforcement on
Before 1st customer      3.2 solicitor review ──► 3.4 pricing decided
Parallel / when ready    3.6 trademark filing
Later / optional         3.7 US copyright reg, 3.8 SPDX headers
```

You can ship the **open-source repo today** — the AGPL side is complete and
correct. The commercial side needs only the placeholder fill (3.1) to be
*presentable*, and the solicitor sign-off (3.2) before you *sign* anything.

---

## 6. Quick reference — "can someone…?"

| They want to… | Under AGPL (free)? | Need commercial licence? |
|---|---|---|
| Self-host unmodified, internally | Yes | No |
| Self-host **modified**, internally only | Yes (no distribution → no §13 trigger*) | No |
| Offer it (modified) as a **public/SaaS** service | Yes, **but must publish their modified source** | Buy to avoid that |
| Bundle it into a **closed-source product** they distribute | No (copyleft forbids) | Yes |
| Use the **name** "Dáil Tracker" commercially | — | Trademark permission |
| Resell **the data** | No (not ours to licence) | N/A — see data source licences |

\* AGPL §13 specifically extends the source-offer duty to users interacting with
the software **over a network**. Confirm edge cases with your solicitor.

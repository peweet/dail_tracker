# Public-Money Pages — Merge / Consolidation Plan (PLAN ONLY)

**Status:** design only — **no code changes**. Written 2026-06-16.
**Question asked:** should the four "public money" pages be merged, and if so how?
**Scope:** `payments.py`, `public_payments.py`, `council_spending.py`, `procurement.py`.
**Companion docs (do not duplicate — this one is the merge decision):** `doc/MONEY_PAGES_SHAPE_BRIEF.md` (the Hub/declutter design), `doc/MONEY_FLOW_DATA_AUDIT.md`, `doc/COUNCIL_SPENDING_SHAPE.md`, `doc/PUBLIC_PAYMENTS_FACT_SCHEMA.md`.

---

## 1. The four pages at a glance

| Page | File (LOC) | url_path | Centres on | Money grain | Lifecycle |
|---|---|---|---|---|---|
| **Payments** | `payments.py` (~431) | `rankings-payments` | a **member** (TD/Senator) | PSA allowance to politicians | paid-to-politics |
| **Public Payments** | `public_payments.py` (~510) | `rankings-public-payments` | a **supplier** (company-class) | PO ≥€20k + actual payments, by body | spent / committed |
| **Council Spending** | `council_spending.py` (**69**) | `rankings-council-spending` | **one council** | AFS revenue + AFS capital + PO | budget + transaction |
| **Procurement** | `procurement.py` (~2,976) | `rankings-procurement` | **supplier / authority / CPV** | contract awards (+ realized spend as 2nd lane) | awarded (+ spent/committed) |

All four sit in the **"The Money"** nav group today.

---

## 2. Where the genuine duplication is

There is exactly **one** real duplication, and it is not a data-model overlap — it is a **rendering + entry-point** overlap:

- The **per-council dossier** is drawn by `procurement._render_payments_publisher_profile(...)` and the council index by `procurement._render_councils(...)`.
- **Three pages reach the same rendered dossier** by three different doors:
  - **Council Spending** → geographic index (province bands) → `?paid_publisher=`
  - **Public Payments** → "Public bodies" tab → council card → `?publisher=<council>`
  - **Procurement** → "Who actually gets paid?" → "Local authorities only" toggle → `?paid_publisher=`
- `council_spending.py` is a **69-line shell** that imports `_render_councils` and `_render_payments_publisher_profile` from `procurement.py`. It owns no logic — only a geographic entry point.

Everything else is **distinct by design** (different grain, source, or entity), not duplication.

---

## 3. What must NEVER be merged (and why)

These are hard rails from `MONEY_FLOW_DATA_AUDIT.md`, `project_council_spending_rebuild_2026_06_15`, and `feedback_no_inference_in_app`. A merge that violates any of them recreates the "one big total public spend" antipattern.

1. **Payments (PSA) stays its own lane.** Money *to politicians* ≠ money *from public bodies*. Different source (`payments_full_psa.parquet`), different entity (member, not supplier), zero shared data. Merging it into a procurement/payments page would be a category error.
2. **AWARDED ≠ SPENT/COMMITTED.** A contract award (eTenders/TED) is a ceiling/intent; a published payment is money that left one body. Never one total. (Procurement vs Public Payments.)
3. **AFS budget ≠ transaction-level PO.** AFS is audited, service-level, the *whole* budget; PO is the named-supplier slice ≥€20k (typically 5–15% coverage). Never summed. (Council Spending lanes.)
4. **eTenders ≠ TED (never UNION-sum).** ~66% name overlap; unioning double-counts. Display-separated only.
5. **SPENT vs COMMITTED never stacked** — side-by-side pills ("€A paid · €B ordered"), labelled an indicative floor.

**Consequence:** the tempting "merge Procurement + Public Payments into one money page" is **rejected** — it collides AWARDED with SPENT and produces a 6–7 tab monster. The right wayfinding answer is the **Hub** in `MONEY_PAGES_SHAPE_BRIEF.md`, not a page merge.

---

## 4. Recommendation

**Do not do a big merge.** Do one small, safe consolidation + lean on the already-designed Hub.

### 4a. RECOMMENDED — fold Council Spending into Public Payments as a geographic tab (low risk)
**Why:** Council Spending is a 69-LOC shell over Procurement's renderers; it is an *entry point*, not a feature. Public Payments is the natural civic home for "my council".

**Steps (when executed later):**
1. Add a third tab to `public_payments.py`: **"My council"** (province bands index), calling the *existing* `procurement._render_councils` / `_render_payments_publisher_profile` — no renderer rewrite.
2. Keep Procurement's "Local authorities only" toggle exactly as-is (it filters the supplier/body ranking; it is not the same as the geographic index).
3. Redirect the old `rankings-council-spending` url_path → `rankings-public-payments?tab=council` (preserve inbound links; add to the legacy-redirect handler).
4. Delete `council_spending.py` and its nav entry. Net: **−1 page, −69 LOC, one fewer "which page do I open?"**
5. Update `COUNCIL_SPENDING_SHAPE.md` to note the dossier now lives under Public Payments.

**Risk:** low. Same renderer, same data, same never-sum lanes. Only the door moves.

### 4b. RECOMMENDED — build the "Follow the money" Hub (already designed)
This is the real fix for "I can't tell these pages apart." It is **already specified** in `MONEY_PAGES_SHAPE_BRIEF.md` (lane cards: Awarded / Paid out / To politics; entry cards: Company / Department / Category / Council; one coverage sentence). This plan does not re-spec it — it endorses building it as the front door, after 4a.

### 4c. REJECTED — merge Procurement + Public Payments
Grain conflict (AWARDED vs SPENT) + tab explosion. Keep separate; let the Hub route between them.

### 4d. REJECTED — merge Payments (PSA) with anything
Separate money lane. Keep standalone.

---

## 5. Sequencing

| Order | Action | Effort | Depends on |
|---|---|---|---|
| 1 | Build the Hub (4b) per `MONEY_PAGES_SHAPE_BRIEF.md` | medium | — |
| 2 | Fold Council Spending → Public Payments "My council" tab (4a) | low | Hub nav exists |
| 3 | (Optional, future) cross-register **entity dossier** unifying a supplier's awards + payments + lobbying + CRO — requires a hardened `dim_supplier` spine first | high | dim_supplier |

Do **not** start with the merge — start with the Hub, because the Hub is what makes the four-lane separation legible. The Council Spending fold is cosmetic cleanup that rides on the Hub's nav.

---

## 6. Acceptance checks for any future execution
- No view or card ever sums across AWARDED / SPENT / AFS / PSA / eTenders+TED.
- `rankings-council-spending` old links still resolve (redirect).
- Procurement page diff = nav/redirect only (no data-model change — explicit out-of-scope in `MONEY_PAGES_SHAPE_BRIEF.md`).
- Tier pills remain side-by-side; coverage gaps (AFS arrears, <€20k unpublished) still labelled.
- pytest + SQL-contract tests green; pages render on a freshly-restarted Streamlit.

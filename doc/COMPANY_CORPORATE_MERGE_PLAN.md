# Company vs Corporate Pages — Merge Plan (PLAN ONLY)

**Status:** design only — **no code changes**. Written 2026-06-16.
**Question asked:** should `company.py` and `corporate.py` be merged?
**TL;DR:** **No — keep them separate. The problem is the naming collision, not the architecture. Rename, don't merge.**

---

## 1. The two pages

| | **Company** (`company.py`) | **Corporate Notices** (`corporate.py`) |
|---|---|---|
| LOC | ~217 | ~2,496 |
| url_path | `company` (**hidden** in nav) | `rankings-corporate` (**visible**, "Law & Records") |
| Reached from | Procurement / Public Payments **supplier cards** | Main nav, standalone report |
| Centres on | a **procurement supplier** | a **company named in an insolvency notice** |
| Join id | `supplier_norm` (from eTenders) | `entity_norm` (computed from Iris text) |
| Data source | `v_procurement_*` (eTenders awards, body payments, TED) | `v_corporate_*` ← `corporate_notices.parquet` (Iris Oifigiúil) |
| Answers | "what public money has this firm won / been paid?" | "is this company in receivership / examinership / liquidation, and who appointed the receiver?" |

---

## 2. Are they two views of one spine? No.

- **Different data worlds:** procurement register (eTenders + payments + TED) vs the official gazette (Iris Oifigiúil insolvency notices).
- **Different identifiers:** `supplier_norm` (eTenders supplier strings) vs `entity_norm` (normalised from Iris notice text). The normalisation regex is *similar* (legal-form strip + lowercase) but the upstream pipelines are entirely separate and **no SQL view joins them**.
- **Different lifecycle questions:** "how much did they win/get paid?" vs "are they failing / being rescued / who's calling in the loan?"
- **Different audiences:** procurement/supplier researchers vs insolvency watchers (journalists, finance, distressed-asset trackers).
- **Different privacy boundaries:** `company.py` shows supplier entities incl. legitimate sole-traders (procurement context); `corporate.py` **excludes personal insolvency by policy** (`feedback_personal_insolvency_privacy`). Merging risks bleeding a personal-bankruptcy signal into a procurement surface — a real privacy hazard.

A supplier that later goes into receivership is a genuine but **rare intersection story**, not the primary job of either page. There is no canonical CRO join today that would make that intersection reliable rather than a soft name-match (which would manufacture false "won a contract, then failed" correlations — a `feedback_no_inference_in_app` violation).

---

## 3. The actual problem: the names imply an overlap that doesn't exist

"Company" and "Corporate" sound like the same thing, so a user reasonably expects them to be related — and they aren't. This is a **labelling** defect, not a structural one.

### Recommended fix — rename for intent, keep separate
- **`company.py` → "Supplier profile" / "Supplier dossier"** (it is the entity-first procurement supplier flagship; it is already hidden, so renaming the title + any breadcrumb is low-blast-radius and the url_path can stay `company` or become `supplier`).
- **`corporate.py` → keep "Corporate Notices"** (already accurate — it is insolvency/gazette, in "Law & Records"). Optionally subtitle it "Receiverships, examinerships & liquidations (Iris Oifigiúil)" to remove any ambiguity with the supplier profile.

This removes the confusion at the source with near-zero risk and no data-model work.

---

## 4. Why NOT to merge (risks if attempted)

1. **No join exists** — a merged "entity" page would need an eTenders×Iris CRO join that isn't built; a name-match is soft and would mislead.
2. **Privacy regression** — personal-insolvency exclusion could be undermined in a procurement context.
3. **Audience + nav collision** — one is intentionally hidden (drill-target), the other is a discoverable standalone report; merging breaks both entry models.
4. **Search ambiguity** — "Acme Ltd" would have to guess: awards or insolvency?
5. **2,700-LOC two-audience page** with colliding CSS namespaces (`pr-*` vs `corp-*`) and two different editorial honesty stories ("three registers never summed" vs "appointer/operator analysis").

---

## 5. The only merge worth considering (future, conditional)

A **cross-register Entity Dossier** that, for a given CRO number, shows *tabs* for: procurement (awards+payments), lobbying, charity status, **and** any corporate-distress notices. This is the `MONEY_PAGES_SHAPE_BRIEF.md` "parametrised entity dossier" idea extended to insolvency.

**Hard precondition:** a canonical **`dim_supplier` / CRO spine** that reliably links `supplier_norm` ↔ `entity_norm` ↔ CRO number. Until that exists, this dossier would be built on a soft name-match and must not ship. Treat as **P3, blocked on dim_supplier** — and even then it *links* the two registers in one profile rather than *deleting* either page.

---

## 6. Recommendation summary

| Option | Verdict |
|---|---|
| Full merge into one page | **Reject** (no join, privacy, audience, search ambiguity) |
| Partial merge (shared header + tabs) | **Reject** (false "same company" premise; needs a join that doesn't exist) |
| **Rename `company.py` → "Supplier profile"; keep both separate** | **Recommended** (kills the naming confusion at zero data risk) |
| Future cross-register Entity Dossier | **Defer to P3**, blocked on a canonical `dim_supplier`/CRO spine |

**Net:** the gut-feel "company + corporate look like duplicates" is a **name** problem. Fix the label, leave the architecture. Document references: `doc/corporate_feature.md`, `doc/APP_REDESIGN_SWEEP_2026_06_10.md` §2, `feedback_personal_insolvency_privacy`.

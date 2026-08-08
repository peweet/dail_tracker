"""Memory garbage collection — report rot in the auto-memory store; archive on request.

The memory dir only grows (275 files as of 2026-07-25) and recall quality decays as
stale or orphaned cards accumulate — the MemoryBank/decay idea from the agent-memory
literature, applied to this repo's file memory. This tool REPORTS by default and only
moves files with --archive (never deletes: feedback_archive_dont_delete). It never
touches MEMORY.md/MEMORY_COLD.md content, and anything linked from the HOT index or
carrying safety language is never an archive candidate.

What it flags:
  orphans        — memory files linked from NEITHER index (unreachable by recall
                   except via FTS; usually a forgotten index line, sometimes rot)
  stale          — not modified in --stale-days (default 60) AND not hot-linked
  broken_links   — [[name]] references whose target file doesn't exist
  corrected      — files whose body contains a correction/overturned marker; their
                   headline claim may no longer hold — re-verify before citing

Currency band — every card also gets a graded Verified/Reported/Extracted/Indicative
band, the SAME 4-band scale `services/data_contracts.py` uses for data trust ("one
vocabulary for the whole project", never a second scale), collapsed by the same
weakest-link `collapse_tiers`. See reference_agent_memory_staleness_literature_2026_07_31
in persistent memory for the literature this is grounded in (Park et al. 2023,
Zhong et al. 2024, Rasmussen et al. 2025) and what was deliberately left out (LLM-judged
contradiction detection — Zep's actual mechanism — is a named STILL OPEN, not built here).
Components (each a ceiling; worst wins):
  age_ceiling       — graded age buckets, not a single cliff (MemoryBank-shaped decay:
                      <30d A, 30-90d B, 90-180d C, >180d D)
  correction_ceiling— CORRECTED_RE hit OR an explicit `superseded_by:` frontmatter
                      field -> D (the deterministic subset of Zep-style invalidation)
  link_ceiling      — hot-linked (MEMORY.md HOT tier) -> A, cold-linked only -> B,
                      orphan -> C (Generative-Agents' importance term, using this
                      repo's existing hand-curated hot/cold split as the signal)
  broken_link_ceiling — this card's own [[links]] point at a missing file -> C

Use --min-band to surface only cards at or below a band before citing one as fact.

Usage:
  python tools/memory_gc.py                        # report only
  python tools/memory_gc.py --archive               # move orphan+stale files to archive/
  python tools/memory_gc.py --stale-days 90
  python tools/memory_gc.py --min-band Extracted    # cards worth re-verifying
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.data_contracts import TRUST_TIER_LABEL, TRUST_TIERS, collapse_tiers  # noqa: E402

MEM = Path.home() / ".claude" / "projects" / "c--Users-pglyn-PycharmProjects-dail-extractor" / "memory"
ARCHIVE = MEM / "archive"
INDEXES = ("MEMORY.md", "MEMORY_COLD.md")
LINK_PREFIXES = ("feedback_", "project_", "reference_")
SAFETY_RE = re.compile(r"never[- ]sum|privacy|quarantine|SAFETY|never push|PII", re.I)
CORRECTED_RE = re.compile(r"⚠?\s*CORRECTED|overturned|superseded|FALSE ALARM|invalid(?:ated)?\b", re.I)
LINK_RE = re.compile(r"\[\[([\w-]+)\]\]")
MDLINK_RE = re.compile(r"\]\(([\w-]+)\.md\)")
SUPERSEDED_BY_RE = re.compile(r"^superseded_by:\s*(\S+)\s*$", re.M)
FRONTMATTER_RE = re.compile(r"\A---\n(.*?\n)---\n", re.S)

# Rank derived from TRUST_TIERS order (A strongest .. D weakest) — no private import needed.
_TIER_RANK = {t: len(TRUST_TIERS) - 1 - i for i, t in enumerate(TRUST_TIERS)}
_LABEL_TO_TIER = {label: tier for tier, label in TRUST_TIER_LABEL.items()}


def _age_ceiling(age_d: float) -> str:
    if age_d < 30:
        return "A"
    if age_d < 90:
        return "B"
    if age_d < 180:
        return "C"
    return "D"


def _frontmatter(body: str) -> str:
    m = FRONTMATTER_RE.match(body)
    return m.group(1) if m else ""


RECALL_PAT = re.compile(rb"memory[/\\]([\w-]{1,120})\.md")
RECALL_CACHE = Path(__file__).resolve().parent.parent / "logs" / "memory_recall_cache.json"


def _recall_ages() -> dict[str, float]:
    """Newest transcript-mention age (days) per card stem — MemoryBank's reinforcement
    rule (AAAI 2024, §2.3: recall resets the decay clock), with session transcripts as
    the deterministic recall signal; no LLM involved.

    Matches PATH references (``memory/<stem>.md``) only, never bare stems: memory_gc's
    own report prints bare stems into the transcript of whichever session runs it, so
    stem-matching would reinforce every card on every run — a feedback loop that
    defeats the decay. Path references occur only on genuine touches (Read/Edit of the
    card, auto-memory injection). MEMORY.md index lines link as ``(stem.md)`` with no
    ``memory/`` prefix, so being indexed doesn't count as recall either.

    The transcript's mtime stands in for the mention time (a mention is never newer
    than its file). Incremental: per-transcript stem sets cached by (mtime, size) in
    logs/memory_recall_cache.json (gitignored), so steady-state cost is only the
    sessions active since the last run; the first full pass reads every transcript.
    Fails open to {} — currency banding must survive a broken cache or transcript.
    """
    try:
        try:
            cache = json.loads(RECALL_CACHE.read_text(encoding="utf-8"))
        except Exception:
            cache = {}
        new_cache: dict[str, dict] = {}
        newest: dict[str, float] = {}
        for f in MEM.parent.glob("*.jsonl"):
            st = f.stat()
            ent = cache.get(f.name)
            if ent and ent.get("mtime") == st.st_mtime and ent.get("size") == st.st_size:
                stems = ent["stems"]
            else:
                stems = sorted({m.group(1).decode("utf-8", "replace") for m in RECALL_PAT.finditer(f.read_bytes())})
            new_cache[f.name] = {"mtime": st.st_mtime, "size": st.st_size, "stems": stems}
            for s in stems:
                if st.st_mtime > newest.get(s, 0.0):
                    newest[s] = st.st_mtime
        RECALL_CACHE.parent.mkdir(exist_ok=True)
        RECALL_CACHE.write_text(json.dumps(new_cache), encoding="utf-8")
        now = time.time()
        return {s: (now - ts) / 86400 for s, ts in newest.items()}
    except Exception:
        return {}


def resolve_link(target: str, names: set[str], archived: set[str]) -> tuple[str, str | None]:
    """Classify one [[target]] as live / archived / alias / broken.

    Archiving a card never deletes it (feedback_archive_dont_delete), so a link into
    memory/archive/ still resolves — counting those as broken inflated the rot signal
    with 76 of 94 false positives (measured 2026-08-08) and, via broken_link_ceiling,
    dragged the currency band of every card that had ever linked to an archived note.

    `alias` covers the kebab-case/prefix-less slugs that predate the naming convention
    (``[[dual-config-files]]`` -> ``feedback_dual_config_files``): the card exists, the
    reference just spells it the old way, so it is a rename to apply, not rot.
    """
    if target in names:
        return ("live", target)
    if target in archived:
        return ("archived", target)
    norm = target.replace("-", "_").lower()
    by_norm = {n.replace("-", "_").lower(): n for n in names}
    if norm in by_norm:
        return ("alias", by_norm[norm])
    for prefix in LINK_PREFIXES:
        if prefix + norm in by_norm:
            return ("alias", by_norm[prefix + norm])
    return ("broken", None)


def _headline(body: str) -> str:
    """Frontmatter + opening ~400 chars of the body — where a card states its OWN
    headline claim (e.g. ``⚠ CORRECTED 2026-07-25 — ...``). CORRECTED_RE is scanned
    here, not the whole body: full-body scanning fires on prose that mentions some
    OTHER thing being superseded/corrected/invalid (a data source, a doc, a baseline)
    which is not a claim about this card — measured empirically at 72/328 cards
    false-positiving on bare "corrected"/"superseded" in unrelated sentences, cut to
    9 (all genuine) once scoped to the headline.
    """
    m = FRONTMATTER_RE.match(body)
    if m:
        return body[: m.end()] + body[m.end() : m.end() + 400]
    return body[:400]


def currency_band(name: str, body: str, age_d: float, hot_linked: set, linked: set, has_broken_link: bool) -> dict:
    """Grade one memory card on the 4-band scale by weakest-link over currency components."""
    fm = _frontmatter(body)
    superseded_by = SUPERSEDED_BY_RE.search(fm)
    corrected = bool(CORRECTED_RE.search(_headline(body))) or superseded_by is not None

    if name in hot_linked:
        link_tier = "A"
    elif name in linked:
        link_tier = "B"
    else:
        link_tier = "C"

    components = {
        "age_ceiling": _age_ceiling(age_d),
        "correction_ceiling": "D" if corrected else "A",
        "link_ceiling": link_tier,
        "broken_link_ceiling": "C" if has_broken_link else "A",
    }
    tier, binding = collapse_tiers(components)
    return {
        "tier": tier,
        "label": TRUST_TIER_LABEL[tier],
        "components": components,
        "binding": binding,
        "superseded_by": superseded_by.group(1) if superseded_by else None,
    }


def scan(stale_days: int) -> dict:
    index_text = ""
    for ix in INDEXES:
        p = MEM / ix
        if p.exists():
            index_text += p.read_text(encoding="utf-8")
    hot_text = (MEM / "MEMORY.md").read_text(encoding="utf-8") if (MEM / "MEMORY.md").exists() else ""
    linked = set(MDLINK_RE.findall(index_text))
    hot_linked = set(MDLINK_RE.findall(hot_text))

    cards = {}
    for f in sorted(MEM.glob("*.md")):
        if f.name in INDEXES:
            continue
        body = f.read_text(encoding="utf-8", errors="replace")
        cards[f.stem] = {"path": f, "body": body, "age_d": (time.time() - f.stat().st_mtime) / 86400}

    names = set(cards)
    orphans = sorted(n for n in names if n not in linked)
    stale = sorted(
        n
        for n, c in cards.items()
        if c["age_d"] > stale_days and n not in hot_linked and not SAFETY_RE.search(c["body"])
    )
    index_stems = {Path(ix).stem for ix in INDEXES}
    archived_stems = {f.stem for f in ARCHIVE.glob("*.md")} if ARCHIVE.exists() else set()
    broken_set: set[str] = set()
    archived_refs: set[str] = set()
    alias_refs: set[str] = set()
    for n, c in cards.items():
        for t in LINK_RE.findall(c["body"]):
            if t in index_stems:
                continue
            kind, resolved = resolve_link(t, names, archived_stems)
            if kind == "broken":
                broken_set.add(f"{n} -> [[{t}]]")
            elif kind == "archived":
                archived_refs.add(f"{n} -> [[{t}]]")
            elif kind == "alias":
                alias_refs.add(f"{n} -> [[{t}]] (rename to [[{resolved}]])")
    broken = sorted(broken_set)
    corrected = sorted(n for n, c in cards.items() if CORRECTED_RE.search(c["body"]))
    # archive candidates: orphaned AND stale AND not safety/hot — the strictest cut
    candidates = sorted(set(orphans) & set(stale))

    broken_names = {b.split(" -> ", 1)[0] for b in broken}
    # Effective age = min(mtime age, newest transcript recall age) — MemoryBank's
    # reinforcement rule. Applied to the currency band ONLY; the legacy stale/archive
    # lists stay mtime-based so archive candidacy doesn't shift under this change.
    recall = _recall_ages()
    currency = {
        n: currency_band(
            n, c["body"], min(c["age_d"], recall.get(n, float("inf"))), hot_linked, linked, n in broken_names
        )
        for n, c in cards.items()
    }
    # Feedback cards (behavioral corrections) never once recalled in any transcript —
    # the write-heavy/read-light gap measured 2026-07-31 (95/334 cards ever recalled).
    # A correction that is never re-read never changes behavior: triage these into
    # promote-to-HOT-one-liner or archive.
    never_recalled_feedback = sorted(n for n in cards if n.startswith("feedback_") and n not in recall)
    return {
        "total": len(cards),
        "orphans": orphans,
        "stale": stale,
        "broken_links": broken,
        "archived_link_refs": sorted(archived_refs),
        "alias_link_refs": sorted(alias_refs),
        "corrected": corrected,
        "archive_candidates": candidates,
        "currency": currency,
        "never_recalled_feedback": never_recalled_feedback,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--archive", action="store_true", help="move archive_candidates into memory/archive/ (never deletes)"
    )
    ap.add_argument("--stale-days", type=int, default=60)
    ap.add_argument(
        "--min-band",
        choices=list(TRUST_TIER_LABEL.values()),
        default=None,
        help="only list currency entries AT OR BELOW this band (e.g. Extracted surfaces Extracted+Indicative)",
    )
    args = ap.parse_args()

    r = scan(args.stale_days)
    print(f"memory cards: {r['total']}  (indexes + archive excluded)")
    for key in ("orphans", "stale", "corrected", "archive_candidates"):
        vals = r[key]
        print(f"\n{key} ({len(vals)}):")
        for n in vals[:30]:
            print(f"  {n}")
        if len(vals) > 30:
            print(f"  ... and {len(vals) - 30} more")
    print(f"\nbroken [[links]] ({len(r['broken_links'])}) — target does not exist anywhere:")
    for b in r["broken_links"][:20]:
        print(f"  {b}")
    print(f"\nlinks into memory/archive/ ({len(r['archived_link_refs'])}) — resolve fine, not rot")
    print(f"old-style slugs to rename ({len(r['alias_link_refs'])}):")
    for a in r["alias_link_refs"][:20]:
        print(f"  {a}")

    nrf = r["never_recalled_feedback"]
    print(
        f"\nnever-recalled feedback cards ({len(nrf)}) — corrections that never changed behavior; promote or archive:"
    )
    for n in nrf[:60]:
        print(f"  {n}")

    threshold_rank = _TIER_RANK[_LABEL_TO_TIER[args.min_band]] if args.min_band else None
    currency_items = sorted(r["currency"].items(), key=lambda kv: _TIER_RANK[kv[1]["tier"]])
    if threshold_rank is not None:
        currency_items = [(n, c) for n, c in currency_items if _TIER_RANK[c["tier"]] <= threshold_rank]
    print(f"\ncurrency band ({len(currency_items)}{' filtered' if threshold_rank is not None else ''}):")
    for n, c in currency_items[:60]:
        suffix = f" -> superseded_by: {c['superseded_by']}" if c["superseded_by"] else ""
        print(f"  [{c['label']} — {','.join(c['binding'])}] {n}{suffix}")
    if len(currency_items) > 60:
        print(f"  ... and {len(currency_items) - 60} more")

    if args.archive and r["archive_candidates"]:
        dest = MEM / "archive"
        dest.mkdir(exist_ok=True)
        moved = []
        for n in r["archive_candidates"]:
            src = MEM / f"{n}.md"
            try:
                src.rename(dest / src.name)
                moved.append(n)
            except OSError as exc:
                print(f"  SKIP {n}: {exc}")
        (dest / "_archive_log.jsonl").open("a", encoding="utf-8").write(
            json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "moved": moved}) + "\n"
        )
        print(f"\narchived {len(moved)} card(s) to memory/archive/ — restore by moving back")
    elif args.archive:
        print("\nnothing meets the archive bar (orphaned AND stale AND non-safety)")


if __name__ == "__main__":
    main()

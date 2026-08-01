"""Classify council-minutes business into the STATUTORY power taxonomy + topic flags.

EXPERIMENTAL sandbox. Two passes over the clean corpus:
  1. Doc-level POWER EVENTS — regex-seeded detection of the statutorily distinct classes
     (reference_local_government_domain / doc/LOCAL_DEMOCRACY_OVERRIDE_RESEARCH.md):
       reserved_real     budget adoption, development plan/variation, LPT factor, ARV/rates
       reserved_veto     s.183 land disposal, Part 8 / s.179 own-development (PROCEED BY
                         DEFAULT — absence of a blocking motion is consent, so detection
                         means "item was before members", not "members decided")
       reserved_super    material contravention (3/4 of total membership, PDA s.34(6))
       exec_noted        s.136 CE/management report, annual report noting
       requisition       s.140 members-direct-the-CE notices (rare post-2014; planning excluded)
  2. Motion-level TOPICS + CONCERN flags over decisions.jsonl — keyword taxonomy for the
     issues councillors raise (housing, roads, water, dereliction, ...) and a concern flag
     for calls-on-government motions ("That this Council calls on the Minister ...").

Outputs: power_events.jsonl · motion_topics.jsonl · VALUE_ASSESSMENT.md (per-council power
mix as DESCRIPTIVE COUNTS — deliberately NO composite "power score"; the never-blend rule
applies to override channels and the promote gate applies before any UI use).

Usage: python minutes_value_classify.py
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ── power taxonomy (statute-anchored regexes; case-insensitive) ─────────────────
POWER_EVENTS = {
    "reserved_real_budget": r"adopt(?:ion|ed)?[^.\n]{0,80}budget|statutory budget meeting|budget 20\d\d[^.\n]{0,40}adopt",
    "reserved_real_devplan": r"(development plan|variation no|proposed variation)[^.\n]{0,120}(adopt|approv|vari)|material alteration",
    "reserved_real_lpt": r"local property tax|lpt[^a-z]|local adjustment factor",
    "reserved_real_rates": r"annual rate on valuation|\barv\b|commercial rates[^.\n]{0,60}(set|adopt|determin)",
    # widened 2026-08-01: AB_COMPARISON.md — 10/10 sampled SVM flags were real disposals
    # phrased without the section number (Wicklow "To consider the disposal of 0.43 acres…")
    "reserved_veto_s183": r"section\s*183|s\.?\s*183[^0-9]|disposal of (land|property|propert|fee simple)|"
                          r"(to consider|proposed) (the )?disposal of",
    "reserved_veto_part8": r"part\s*(8|viii)\b|section\s*179|s\.?\s*179[^0-9]",
    "reserved_super_matcontra": r"material contravention",
    "exec_noted_mgmt": r"(management report|chief executive.{0,25}report)[^.\n]{0,80}(noted|received)|section\s*136",
    # widened 2026-08-01: AB_COMPARISON.md — 4/4 sampled flags real (audit-committee reports
    # "circulated with the agenda" carry no adopt/noted verb in range)
    "exec_noted_annual": r"annual report[^.\n]{0,60}(adopt|noted|approv|circulated|received)|"
                         r"audit committee annual report",
    "requisition_s140": r"section\s*140|s\.?\s*140[^0-9]",
}

# ── topic taxonomy for motions/concerns ────────────────────────────────────────
TOPICS = {
    "housing": r"housing|homeless|tenant|social hous|affordable|dereliction|derelict|vacant (home|propert)",
    "roads_traffic": r"\broad\b|footpath|traffic|speed limit|junction|pothole|car park|parking",
    "water_sewer": r"uisce|irish water|sewerage|wastewater|water suppl|flood",
    "planning_enforcement": r"unauthorised development|enforcement|quarry|derogation",
    "environment_climate": r"climate|biodiversity|tree|hedgerow|renewable|solar|wind farm|litter|illegal dump",
    "health_services": r"hospital|ambulance|\bhse\b|mental health|disability service",
    "policing_safety": r"garda|antisocial|anti-social|cctv|crime",
    "economic_tourism": r"tourism|town centre|regeneration|enterprise|jobs|broadband",
    "burial_community": r"burial|cemeter|graveyard|community centre|playground|library",
    "irish_language_culture": r"gaeilge|irish language|gaeltacht|heritage|festival",
    "immigration_ipas": r"ipas|international protection|asylum|direct provision|ukrain",
    "energy_infrastructure": r"eirgrid|pylon|substation|data centre|grid",
}
CONCERN = re.compile(
    r"calls? (up)?on the (minister|government|taoiseach|department|hse|tii|uisce)|"
    r"write to the (minister|department|taoiseach)|urges the (minister|government)", re.I)

# ── community-opposition lens (2026-08-01, extended from the CHASE/Ringaskiddy find) ──
# Two capture families: EVENT markers (how opposition reaches the chamber) and named
# GROUP patterns (who). A group hit without an event marker still counts — the name
# form itself ("X Action Group", "Save X") is the signal. Extracted band; group-name
# precision is unmeasured until a P(True) sample runs (state it in the report).
OPPOSITION_EVENTS = {
    "deputation": r"\bdeputation\b",
    "petition": r"\bpetition(?:s|ed)?\b",
    "objection": r"\bobject(?:ion|ing|ors)s?\s+(?:to|against|were|was|raised)\b",
    "protest": r"\bprotest(?:s|ers|ed)?\b|\bdemonstration\b",
    "residents_concerns": r"residents'?\s+(?:concerns?|fears?|opposition|objections?)|"
                          r"concerns?\s+(?:of|raised by|expressed by)\s+(?:local\s+)?residents",
    "public_meeting": r"\bpublic meeting\b",
}
GROUP_NAME = re.compile(
    r"\b([A-Z][\w'’&\- ]{2,40}?(?:Residents'? Association|Residents'? Group|Action Group|"
    r"Community (?:Association|Group|Council|Alliance)|Tidy Towns|Alliance for [A-Z][\w' ]{2,30}|"
    r"Campaign(?: Group)?)|Save [A-Z][\w'’\- ]{2,30}|Friends of (?:the )?[A-Z][\w'’\- ]{2,30})\b")
_OPP_RX = {k: re.compile(v, re.I) for k, v in OPPOSITION_EVENTS.items()}

_POWER_RX = {k: re.compile(v, re.I) for k, v in POWER_EVENTS.items()}
_TOPIC_RX = {k: re.compile(v, re.I) for k, v in TOPICS.items()}


def main() -> int:
    docs = [json.loads(l) for l in (HERE / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    # canonicalise the June label drift so per-council tables don't split Galway County
    for d in docs:
        if d["local_authority"] == "Galway":
            d["local_authority"] = "Galway County"

    # pass 1: doc-level power events
    events = []
    for d in docs:
        p = HERE / d.get("text_path", "")
        if not d.get("text_path") or not p.exists():
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        for cls, rx in _POWER_RX.items():
            hits = rx.findall(text)
            if hits:
                events.append({
                    "local_authority": d["local_authority"], "meeting": d["meeting"],
                    "doc_type": d.get("doc_type"), "power_class": cls, "n_hits": len(hits),
                })
    (HERE / "power_events.jsonl").write_text(
        "\n".join(json.dumps(e, ensure_ascii=False) for e in events), encoding="utf-8")

    # pass 2: motion topics + concern flags
    motions = [json.loads(l) for l in (HERE / "decisions.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    tagged = []
    for m in motions:
        blob = f"{m.get('item_context', '')} {m.get('motion_snippet', '')}"
        topics = [t for t, rx in _TOPIC_RX.items() if rx.search(blob)]
        la = m["local_authority"] if m["local_authority"] != "Galway" else "Galway County"
        tagged.append({
            "local_authority": la, "meeting": m["meeting"], "meeting_date": m.get("meeting_date", ""),
            "proposer": m.get("proposer", ""), "outcome": m.get("outcome", ""),
            "topics": topics, "is_concern_to_govt": bool(CONCERN.search(blob)),
            "snippet": m.get("motion_snippet", "")[:180],
        })
    (HERE / "motion_topics.jsonl").write_text(
        "\n".join(json.dumps(t, ensure_ascii=False) for t in tagged), encoding="utf-8")

    # pass 3: community-opposition events + named groups (snippet grain).
    # MIN_VOTE_YEAR cutoff applies HERE too: without it the Louth 1900s minute-books
    # flood 'deputation' (standard practice then) — 3 of 6 in the first P(True) spot
    # sample were minute-book rows. Same year rule as the votes harness: a doc with no
    # provable >=2018 year is excluded from this lens.
    from extractors.council_votes_extract import MIN_VOTE_YEAR, _vote_year

    opp_rows = []
    for d in docs:
        p = HERE / d.get("text_path", "")
        if not d.get("text_path") or not p.exists():
            continue
        y = _vote_year(d)
        if y is None or y < MIN_VOTE_YEAR:
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        la = d["local_authority"]
        for marker, rx in _OPP_RX.items():
            for m in rx.finditer(text):
                snip = " ".join(text[max(0, m.start() - 130): m.start() + 220].split())
                g = GROUP_NAME.search(text[max(0, m.start() - 300): m.start() + 300])
                opp_rows.append({
                    "local_authority": la, "meeting": d["meeting"],
                    "meeting_date": d.get("meeting_date", ""), "marker": marker,
                    "group": (g.group(0).strip() if g else ""),
                    "topics": [t for t, trx in _TOPIC_RX.items() if trx.search(snip)],
                    "source_status": d.get("status", ""), "snippet": snip[:260],
                })
        # standalone named-group mentions with no event marker nearby
        for g in GROUP_NAME.finditer(text):
            snip = " ".join(text[max(0, g.start() - 130): g.start() + 220].split())
            opp_rows.append({
                "local_authority": la, "meeting": d["meeting"],
                "meeting_date": d.get("meeting_date", ""), "marker": "named_group",
                "group": g.group(0).strip(),
                "topics": [t for t, trx in _TOPIC_RX.items() if trx.search(snip)],
                "source_status": d.get("status", ""), "snippet": snip[:260],
            })
    (HERE / "opposition_events.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in opp_rows), encoding="utf-8")
    opp_by_marker = Counter(r["marker"] for r in opp_rows)
    opp_groups = Counter(r["group"] for r in opp_rows if r["group"])
    opp_topics = Counter(t for r in opp_rows for t in r["topics"])
    n_event = sum(1 for r in opp_rows if r["marker"] != "named_group")
    OL = ["# Community opposition in council minutes — events, groups, concerns\n"]
    OL.append(f"Auto-generated (opposition lens, docs with provable year >= {MIN_VOTE_YEAR} only). "
              f"**{n_event} OPPOSITION-EVENT rows** (deputation/petition/objection/protest/"
              f"residents-concerns/public-meeting) + {len(opp_rows) - n_event} standalone "
              f"named-group mentions (a community-groups DIRECTORY, not opposition per se — "
              f"Tidy Towns dominate it) across "
              f"{len({r['local_authority'] for r in opp_rows})} councils. Extracted band — "
              "marker precision is MIXED ('deputation' also matches procedural correspondence); "
              "P(True) sample pending (doc/EXTRACTION_QUALITY_CHECKLIST.md).\n")
    OL.append("## Signals by marker\n\n| marker | rows |\n|---|---|")
    for k, n in opp_by_marker.most_common():
        OL.append(f"| {k} | {n} |")
    OL.append("\n## Named groups (top 25 by mentions)\n\n| group | mentions |\n|---|---|")
    for g, n in opp_groups.most_common(25):
        OL.append(f"| {g} | {n} |")
    OL.append("\n## Concern themes on opposition rows\n\n| topic | rows |\n|---|---|")
    for t, n in opp_topics.most_common():
        OL.append(f"| {t} | {n} |")
    (HERE / "OPPOSITION.md").write_text("\n".join(OL), encoding="utf-8")

    # summary
    by_class = Counter(e["power_class"] for e in events)
    docs_by_la = Counter(d["local_authority"] for d in docs if d.get("text_path"))
    mix = defaultdict(Counter)
    for e in events:
        grp = e["power_class"].split("_")[0] + "_" + e["power_class"].split("_")[1]
        mix[e["local_authority"]][grp] += 1
    topic_cnt = Counter(t for r in tagged for t in r["topics"])
    topic_councils = defaultdict(set)
    for r in tagged:
        for t in r["topics"]:
            topic_councils[t].add(r["local_authority"])
    concerns = [r for r in tagged if r["is_concern_to_govt"]]

    L = ["# Council business value classification — power taxonomy + topics\n"]
    L.append(f"Auto-generated by minutes_value_classify.py over {len(docs_by_la)} councils / "
             f"{sum(docs_by_la.values())} clean docs and {len(tagged)} extracted motions. "
             "Rule-seeded pass (regex, statute-anchored) — the golden-set ML pass comes next; "
             "treat counts as Extracted-band, not Verified.\n")
    L.append("## Power-event detections (doc-level, all councils)\n\n| class | statutory anchor | docs w/ hits |\n|---|---|---|")
    anchors = {"reserved_real_budget": "ss.102/103", "reserved_real_devplan": "PDA s.12/13",
               "reserved_real_lpt": "Finance (LPT) Act", "reserved_real_rates": "ARV",
               "reserved_veto_s183": "LGA s.183 (silence = consent)",
               "reserved_veto_part8": "PDA s.179/Part 8 (silence = consent)",
               "reserved_super_matcontra": "PDA s.34(6) (3/4 vote)",
               "exec_noted_mgmt": "LGA s.136", "exec_noted_annual": "LGA s.221",
               "requisition_s140": "LGA s.140 (planning excluded)"}
    for cls, n in by_class.most_common():
        L.append(f"| {cls} | {anchors.get(cls, '')} | {n} |")
    L.append("\n## Per-council power mix (docs with >=1 hit; DESCRIPTIVE — no composite score)\n")
    L.append("| council | docs | reserved_real | reserved_veto | reserved_super | exec_noted | requisition |")
    L.append("|---|---|---|---|---|---|---|")
    for la in sorted(mix, key=lambda x: -docs_by_la.get(x, 0)):
        m = mix[la]
        L.append(f"| {la} | {docs_by_la.get(la, 0)} | {m.get('reserved_real', 0)} | "
                 f"{m.get('reserved_veto', 0)} | {m.get('reserved_super', 0)} | "
                 f"{m.get('exec_noted', 0)} | {m.get('requisition_s140', 0)} |")
    L.append("\n## Motion topics (councillor-raised business)\n\n| topic | motions | councils |\n|---|---|---|")
    for t, n in topic_cnt.most_common():
        L.append(f"| {t} | {n} | {len(topic_councils[t])} |")
    L.append(f"\n## Concerns directed at central government: {len(concerns)} motions\n")
    cc = Counter(r['local_authority'] for r in concerns)
    L.append("| council | calls-on-Minister/Govt motions |\n|---|---|")
    for la, n in cc.most_common(15):
        L.append(f"| {la} | {n} |")
    (HERE / "VALUE_ASSESSMENT.md").write_text("\n".join(L), encoding="utf-8")
    print(f"power events: {len(events)} rows ({len(by_class)} classes) | motions tagged: {len(tagged)} "
          f"| topic hits: {sum(topic_cnt.values())} | govt-concern motions: {len(concerns)}")
    print("-> power_events.jsonl, motion_topics.jsonl, VALUE_ASSESSMENT.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

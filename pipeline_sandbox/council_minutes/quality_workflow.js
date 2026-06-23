export const meta = {
  name: 'council-minutes-quality',
  description: 'Exhaustive quality audit, adversarial vote verification, quarantine triage, and gap/CMIS source recovery over the extracted council-minutes corpus (sandbox only, no gold writes)',
  phases: [
    { title: 'Audit', detail: 'per-council extraction-quality + classification audit' },
    { title: 'Verify', detail: 'adversarial per-member vote-attribution verification' },
    { title: 'Recover', detail: 'find machine-readable sources for CMIS/missing/0-clean councils' },
    { title: 'Synthesize', detail: 'consolidated, verified quality assessment' },
  ],
}

const SBX = 'c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/council_minutes'
const RULES = `HARD RULES: never run git; never write to data/gold or promote anything; stay inside ${SBX}; do NOT run PaddleOCR (it crashes this Windows box) — for any OCR use rapidocr-onnxruntime via the venv; keep work bounded and report honestly (label uncertainty, never fabricate a figure).`

const COUNCILS = [
  'Carlow','Cavan','Clare','Cork City','Cork County','Donegal','Dublin City',
  'Dún Laoghaire-Rathdown','Fingal','Galway City','Galway County','Kerry','Kildare',
  'Kilkenny','Laois','Leitrim','Limerick','Longford','Louth','Mayo','Meath','Monaghan',
  'Offaly','Roscommon','Sligo','South Dublin','Tipperary','Waterford','Westmeath',
  'Wexford','Wicklow',
]

const AUDIT_SCHEMA = {
  type: 'object',
  properties: {
    council: { type: 'string' },
    clean_docs: { type: 'integer' },
    quarantined_docs: { type: 'integer' },
    clean_are_really_minutes: { type: 'boolean', description: 'spot-checked: clean texts are genuine meeting minutes' },
    misclassified_quarantine: { type: 'integer', description: 'quarantined docs that are ACTUALLY usable minutes (recoverable)' },
    doc_types_seen: { type: 'array', items: { type: 'string' } },
    has_attendance: { type: 'boolean' },
    has_agenda_items: { type: 'boolean' },
    has_motions_proposer_seconder: { type: 'boolean' },
    has_named_votes: { type: 'boolean' },
    date_range: { type: 'string' },
    extraction_grade: { type: 'string', enum: ['A','B','C','D','F'], description: 'A=clean complete minutes; F=nothing usable' },
    notes: { type: 'string', description: 'concise: what is usable, what is noise/mojibake, what to fix' },
  },
  required: ['council','clean_docs','quarantined_docs','extraction_grade','notes'],
}

const VERIFY_SCHEMA = {
  type: 'object',
  properties: {
    council: { type: 'string' },
    checked_votes: { type: 'integer' },
    tally_matches_source: { type: 'boolean', description: 'the For/Against/Abstain counts match the source minutes' },
    names_correct: { type: 'boolean' },
    issues: { type: 'array', items: { type: 'string' } },
    verdict: { type: 'string', enum: ['reliable','minor_issues','unreliable'] },
  },
  required: ['council','checked_votes','verdict','issues'],
}

const RECOVER_SCHEMA = {
  type: 'object',
  properties: {
    council: { type: 'string' },
    real_source_found: { type: 'boolean' },
    source_url_or_pattern: { type: 'string', description: 'concrete machine-readable minutes URL / ModernGov mgConvert2PDF pattern / API' },
    format: { type: 'string', description: 'pdf | html | moderngov_cmis | none' },
    sample_extracted_ok: { type: 'boolean' },
    effort: { type: 'string', enum: ['trivial','moderate','hard','blocked'] },
    recommended_approach: { type: 'string' },
  },
  required: ['council','real_source_found','effort','recommended_approach'],
}

// ── Phase 1: audit every council from the already-extracted corpus + quarantine ──
phase('Audit')
const audits = await parallel(COUNCILS.map(la => () =>
  agent(
    `Audit extraction quality for **${la}** council. ${RULES}
     Inputs (read them): corpus texts under ${SBX}/corpus/<slug> (slug = council name lowercased, non-alphanumerics→'_'),
     quarantine rows in ${SBX}/quarantine/quarantine.jsonl (filter local_authority=="${la}"),
     clean rows in ${SBX}/meetings_clean.jsonl, and votes in ${SBX}/member_votes_all.jsonl.
     Open 1-2 clean texts and a couple of quarantined ones. Judge: are the clean docs genuine MINUTES
     (attendance + agenda items + motions/decisions), or agendas/plans/standing-orders? Are any
     QUARANTINED docs actually usable minutes that were wrongly rejected (count = misclassified_quarantine)?
     Note mojibake/OCR-noise. Grade the extraction A–F. Return the schema.`,
    { label: `audit:${la}`, phase: 'Audit', schema: AUDIT_SCHEMA }
  ).then(r => r || { council: la, clean_docs: 0, quarantined_docs: 0, extraction_grade: 'F', notes: 'agent failed' })
))

// councils that produced no clean minutes → recovery targets
const gapCouncils = audits.filter(Boolean).filter(a => (a.clean_docs || 0) === 0).map(a => a.council)
// councils with attributed votes → verification targets
const voteCouncils = audits.filter(Boolean).filter(a => a.has_named_votes).map(a => a.council)
log(`Audit done. gap councils (0 clean): ${gapCouncils.join(', ') || 'none'}. vote councils: ${voteCouncils.join(', ') || 'none'}`)

// ── Phase 2: adversarially verify per-member vote attribution ──
phase('Verify')
const verifs = await parallel((voteCouncils.length ? voteCouncils : ['Carlow']).map(la => () =>
  agent(
    `Adversarially VERIFY the per-member roll-call votes extracted for **${la}**. ${RULES}
     Read ${SBX}/member_votes_all.jsonl (filter local_authority=="${la}"). Pick 1-2 distinct meetings.
     Re-fetch the source minutes PDF (url is in ${SBX}/meetings_clean.jsonl for that council), find the
     roll-call vote table in the text, and CHECK: does the extracted For/Against/Abstain tally match the
     minutes' own stated "Result N For, M Against"? Are the member names correct (vs the council roster)?
     Try to REFUTE the extraction. Return the schema with concrete issues.`,
    { label: `verify:${la}`, phase: 'Verify', schema: VERIFY_SCHEMA }
  ).then(r => r || { council: la, checked_votes: 0, verdict: 'unreliable', issues: ['agent failed'] })
))

// ── Phase 3: recover real sources for gap / CMIS / 0-clean councils ──
phase('Recover')
const recoveries = await parallel((gapCouncils.length ? gapCouncils : []).map(la => () =>
  agent(
    `Find the real machine-readable MINUTES source for **${la}** council, which produced 0 usable minutes. ${RULES}
     Many big councils use a ModernGov/CMIS portal (e.g. councilmeetings.<domain>, <name>.moderngov.co.uk,
     ecouncil.<domain>) where minutes are behind mgConvert2PDF.aspx?ID= or ieListDocuments.aspx?CId=&MId= links,
     or publish HTML minute pages. Use WebSearch/WebFetch and Bash (python+requests/bs4 in the venv) to locate a
     RECENT (2024+) full-council minutes document, fetch it, and confirm you can extract its text (fitz for PDF,
     bs4 for HTML). Report the concrete URL/pattern, format, whether a sample extracted OK, and the effort to wire
     it into the harvester. Do NOT mass-download — one sample is enough.`,
    { label: `recover:${la}`, phase: 'Recover', schema: RECOVER_SCHEMA }
  ).then(r => r || { council: la, real_source_found: false, effort: 'blocked', recommended_approach: 'agent failed' })
))

// ── Phase 4: synthesize a verified quality assessment ──
phase('Synthesize')
const summary = await agent(
  `Write a rigorous markdown quality-assessment report for the council-minutes extraction. ${RULES}
   You are given three JSON arrays: AUDITS, VOTE_VERIFICATIONS, RECOVERIES.
   AUDITS=${JSON.stringify(audits)}
   VERIFICATIONS=${JSON.stringify(verifs)}
   RECOVERIES=${JSON.stringify(recoveries)}
   Produce: (1) headline numbers — councils with usable minutes, total clean vs quarantined, vote coverage;
   (2) a per-council table (grade, clean docs, has-votes, notes); (3) vote-attribution reliability findings;
   (4) quarantine triage — how many are genuinely recoverable and why; (5) gap/CMIS recovery — concrete next
   source per missing council with effort; (6) an honest "what this corpus can and cannot support" section
   (agenda? attendance? motions? named votes?) and the top 5 fixes to maximise quality. Be specific and quantified.
   Write the file to ${SBX}/QUALITY_ASSESSMENT_ULTRA.md using Bash/python (utf-8). Return a 10-line executive summary.`,
  { label: 'synthesize', phase: 'Synthesize', effort: 'high' }
)

return { audits, verifs, recoveries, summary }

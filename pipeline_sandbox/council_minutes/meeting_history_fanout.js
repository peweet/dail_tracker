export const meta = {
  name: 'council-meeting-history-fanout',
  description: 'Fan out one agent per council to extract MEETING HISTORY = agenda items per recent meeting (all 31 LAs), then merge into meeting_history.jsonl. Sandbox only, no gold, no git, rapidocr-not-PaddleOCR.',
  phases: [
    { title: 'Extract', detail: 'per-council agenda extraction (fitz / OCR / HTML / ModernGov)' },
    { title: 'Merge', detail: 'consolidate into meeting_history.jsonl + coverage report' },
  ],
}

const SBX = 'c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/council_minutes'
const RULES = `HARD RULES: never run git; never write to data/gold; stay in ${SBX}; OCR only with rapidocr-onnxruntime (NEVER PaddleOCR — it crashes this Windows box); bounded — at most 6 recent (2024+) meetings; report honestly, never fabricate an agenda item.`

const COUNCILS = [
  'Carlow','Cavan','Clare','Cork City','Cork County','Donegal','Dublin City',
  'Dún Laoghaire-Rathdown','Fingal','Galway City','Galway County','Kerry','Kildare',
  'Kilkenny','Laois','Leitrim','Limerick','Longford','Louth','Mayo','Meath','Monaghan',
  'Offaly','Roscommon','Sligo','South Dublin','Tipperary','Waterford','Westmeath',
  'Wexford','Wicklow',
]

const SCHEMA = {
  type: 'object',
  properties: {
    council: { type: 'string' },
    format: { type: 'string', description: 'born_digital_pdf | scanned_ocr | html | moderngov | none' },
    n_meetings: { type: 'integer' },
    meetings: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          date: { type: 'string' },
          source_url: { type: 'string' },
          agenda_items: { type: 'array', items: { type: 'string' } },
        },
        required: ['date', 'agenda_items'],
      },
    },
    note: { type: 'string', description: 'what was found / any blocker (e.g. ModernGov, scanned, no recent minutes)' },
  },
  required: ['council', 'format', 'n_meetings', 'meetings', 'note'],
}

phase('Extract')
const results = await parallel(COUNCILS.map(la => () =>
  agent(
    `Extract the MEETING HISTORY (agenda items per recent meeting) for **${la}** council. ${RULES}
     1. Read this council's seed meetings-page URL from ${SBX}/council_seeds.csv (column seed_url, row local_authority=="${la}").
     2. From that page (crawl one level for year/sub pages if needed), collect up to 6 RECENT (2024+)
        full-council MINUTES or AGENDA documents.
     3. Extract the AGENDA — the list of items tabled (often "ITEM NO. 1 ...", "1. ...", or agenda
        headings). We want WHAT WAS ON THE AGENDA, not decisions/votes. Use: PyMuPDF/fitz for
        born-digital PDFs; rapidocr-onnxruntime for scanned image PDFs (check fitz text length first —
        if near-zero it's scanned); BeautifulSoup for HTML minute pages; for a ModernGov/CMIS portal
        (councilmeetings.* / *.moderngov.co.uk / ecouncil.*) follow mgConvert2PDF.aspx / ieListDocuments
        links to a real minutes PDF. Clean OCR run-together text into readable item titles.
     4. Return the schema: one entry per meeting with date + agenda_items (clean, <=15 items each).
     Use Bash with the project venv python (.venv/Scripts/python) which has requests, bs4, fitz,
     rapidocr_onnxruntime. If a council can't be reached, return n_meetings 0 with an honest note.`,
    { label: `agenda:${la}`, phase: 'Extract', schema: SCHEMA }
  ).then(r => r || { council: la, format: 'none', n_meetings: 0, meetings: [], note: 'agent failed' })
))

phase('Merge')
const merged = await agent(
  `Merge per-council meeting-history results into ${SBX}/meeting_history.jsonl. ${RULES}
   You are given RESULTS (array of {council, format, n_meetings, meetings:[{date,source_url,agenda_items}], note}):
   ${JSON.stringify(results)}
   Using Bash + .venv/Scripts/python: (a) read the EXISTING ${SBX}/meeting_history.jsonl if present;
   (b) for every meeting in RESULTS with non-empty agenda_items, append a line
   {"council","file"(basename of source_url or date),"date","agenda_items"} — DEDUPE by (council,date,file)
   so re-runs don't duplicate; (c) write the file back (utf-8, one JSON per line).
   Then return a concise coverage report: councils now covered, total meetings, total agenda items,
   and which councils still have 0 (with the per-council note). Do NOT touch gold.`,
  { label: 'merge', phase: 'Merge', effort: 'high' }
)

return { results, merged }

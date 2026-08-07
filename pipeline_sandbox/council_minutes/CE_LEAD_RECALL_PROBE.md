# CE-report lead recall probe

Strict net = the shipped `procurement_leads()` anchors. Broad net = a loose
commercial-activity net. The gap bounds what the strict net can miss; it is an
UPPER bound, since the broad net is low-precision by design.

- sentences scanned: 21134
- strict-net leads: 540
- gap sentences (broad hit, strict miss): 951
- labelling queue written: 218 (max 15/council)

| Council | strict | gap | gap share |
|---|---:|---:|---:|
| Carlow | 69 | 93 | 57% |
| Clare | 24 | 98 | 80% |
| Dún Laoghaire-Rathdown | 22 | 18 | 45% |
| Fingal | 84 | 112 | 57% |
| Galway County | 25 | 115 | 82% |
| Kildare | 102 | 157 | 61% |
| Kilkenny | 13 | 18 | 58% |
| Laois | 18 | 37 | 67% |
| Leitrim | 20 | 14 | 41% |
| Louth | 42 | 85 | 67% |
| Offaly | 13 | 33 | 72% |
| South Dublin | 5 | 36 | 88% |
| Tipperary | 44 | 41 | 48% |
| Westmeath | 4 | 9 | 69% |
| Wicklow | 55 | 85 | 61% |

## Next step

`ce_lead_recall_gap.jsonl` is a QUEUE, not a result. Recall is unmeasured until the
queue is labelled P(True) by a reader who has not seen the strict net's output, per
doc/EXTRACTION_QUALITY_CHECKLIST.md. Recall estimate = strict / (strict + true misses),
where true misses are extrapolated from the labelled sample's positive rate.

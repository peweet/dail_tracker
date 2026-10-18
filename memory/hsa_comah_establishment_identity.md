# HSA establishment identity before EPA coordinates

An operator name can describe multiple establishments. Name-token overlap, a shared county or
a deterministic first match cannot establish which EPA facility supplies an HSA site's point.
Coordinate precision and establishment identity are separate properties.

`extractors/hsa_comah_extract.py::match_epa` requires a compatible name and the exact normalized
publisher address, with a unique best candidate. Conflicting addresses and tied candidates stay
unmatched with explicit status and candidate provenance. Preserve source records for review;
do not manufacture a point or treat an unmatched establishment as absent.

Evidence: `test/extractors/test_hsa_comah_identity_2026_10_17.py` uses independently authored small
publisher-record fixtures. Its controls cover cross-site operator names, same-county different
addresses, an exact positive address and an ambiguous tie. Run the existing HSA extractor and
quality-gate neighbors as well. These checks do not reconcile existing published layer snapshots,
establish consultation contours, or confer reuse permission.

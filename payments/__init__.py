"""Parliamentary Standard Allowance (PSA) payments domain: full TAA+PRA PDF
parse → member enrichment.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Step
scripts are run by the payments chain via ``python -m payments.<step>`` so the
repo root (cwd) is on ``sys.path`` and ``import config`` resolves. The parser
(``payments_full_psa_etl``) and enricher (``payments_member_enrichment``) are
also imported directly by the Seanad chain, which reuses the Dáil parsers.
"""

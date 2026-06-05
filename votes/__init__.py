"""Votes domain: normalise raw division JSON → silver, then cross-dataset
enrichment over the silver outputs.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Run via
``python -m votes.<step>`` so the repo root (cwd) is on ``sys.path`` and
``import config`` resolves. ``transform_votes`` must precede ``enrich`` (enrich
reads silver/pretty_votes.csv). Both are also imported directly by the Seanad
chain, which reuses the Dáil parsers.
"""

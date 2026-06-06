"""Pydantic response models for the published API surface.

These are hand-picked PROJECTIONS, not 1:1 mirrors of the marts — a stable public
contract that insulates consumers from view churn. They live in CORE (not api/)
so the same models can validate a file-based pack product. Version-bump only on a
deliberate published-shape change, never on a mart rewrite.
"""

"""Pure cross-domain helpers shared by multiple pipeline domains.

Top-level package (same pattern as ``services/``) so modules resolve as
``from shared.X import ...`` in both pytest (repo root on ``pythonpath``) and
subprocess pipeline steps (repo root is the working directory → on ``sys.path``).
No domain logic, no IO orchestration — just reusable transforms/utilities.
"""

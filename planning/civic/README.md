# Civic planning data

Public planning-data acquisition and analysis for Dáil Tracker.
For the private SpecPlan product, return to [the planning workspace guide](../README.md).

## Find the code

| Task | Starting point |
| --- | --- |
| Ingest national applications and application-site polygons | [planning_applications_ingest.py](extractors/planning_applications_ingest.py) |
| Resume a bounded ArcGIS acquisition and validate its checkpoints | [acquisition.py](acquisition.py) |
| Merge council register supplements without duplicating national rows | [register_supplement.py](register_supplement.py) |
| Build appeal outcomes and the appeal-case spine | [planning_appeal_outcomes.py](extractors/planning_appeal_outcomes.py) |
| Understand the appeal transform and candidate matching | [planning_appeal_vector.py](extractors/planning_appeal_vector.py) |
| Build application decision profiles | [planning_decision_profiles.py](extractors/planning_decision_profiles.py) |
| Extract OPR and Ministerial plan directions | [opr_plan_directions_extract.py](extractors/opr_plan_directions_extract.py) |
| Inspect experimental land-acquisition compensation aggregates | [planning_cpo_compensation.py](extractors/planning_cpo_compensation.py) |

Follow [extractor guidance](extractors/AGENTS.md) before changing or retiring these
modules. The compensation module retains a sandbox designation; confirm its pipeline
integration before relying on its output.

## Experiments

| Question | Probe |
| --- | --- |
| What planning and CPO notices appear in Iris Oifigiúil? | [iris_planning_notices_audit.py](sandbox/iris_planning_notices_audit.py) |
| What can application unit counts establish about Part V exposure? | [planning_partv_liability_probe.py](sandbox/planning_partv_liability_probe.py) |

These `sandbox/` probes retain their own assumptions and limitations. Their results
do not establish a live pipeline contract or an individual application's compliance.

## Data, tests and verification

Outputs normally live in the repository's `data/` tree, rather than beside this code.
Read each producer's output and coverage declarations before using a dataset.
The loose professional-harvest CSVs in `planning/` belong to the separate private workflow.

Use [test/planning/](../../test/planning/) for focused acquisition, register, appeal,
profile and compensation tests. For commands and required checks, follow
[the repository verification guide](../../AGENTS.md#verification) and
[extractor guidance](extractors/AGENTS.md).

Appeal outcomes and OPR/Ministerial plan directions describe different processes;
keep their measures separate. Preserve published provenance and coverage limitations.

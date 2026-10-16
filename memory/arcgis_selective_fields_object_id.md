# Keep the publisher identifier in selective ArcGIS requests

On 2026-10-15, the national planning sites collector completed its identifier
inventory but failed on page zero. The selected GeoJSON `outFields` omitted the
publisher's `OBJECTID`; the public service then returned neither that property
nor GeoJSON `feature.id`. The same one-record query including `OBJECTID` returned
both as integers. The authoritative service was
`https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer/1`.

`ArcGISStagedCollector.collect_layer()` must include the metadata-defined object
identifier in a selective field request before freezing the checkpoint identity.
Wildcard requests keep their existing meaning. Integer, uniqueness and exact
inventory checks remain strict, and the identifier is removed before persisted
planning rows are produced.

The observable contract is
`test/planning/test_planning_acquisition.py::test_selective_fields_keep_publisher_oid_for_validation_then_drop_it`.
Its GeoJSON and custom-identifier JSON cases reproduced the page-zero failure
before the repair. Both passed afterward, together with the acquisition and site
polygon tests: 18 passed. The test also checks the persisted checkpoint request
and real Parquet column boundary. The fake publisher reproduces field-selection
behaviour; the collector validation and Parquet writes are real.

The repair belongs in request construction. Accepting missing identifiers,
casting unverified values, or dropping source-completeness checks would remove
the guarantee the checkpoint is intended to provide.

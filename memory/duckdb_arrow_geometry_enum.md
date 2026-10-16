# Export geometry type names as plain Arrow text

On 2026-10-15, national planning polygon assembly reached the GeoParquet
validation boundary and failed because DuckDB exported `ST_GeometryType` as
an Arrow dictionary with unsigned `uint8` indices. The ingestion environment's
Arrow-to-NumPy conversion rejected that representation. The same conversion
worked in a newer local Arrow build, so a local successful geometry call did
not establish deployment interoperability.

`services.geometry._VALIDATE_SQL` now casts the geometry enum to `VARCHAR`
before Arrow export. Missing input rows remain null and must be excluded before
NumPy sorts distinct type names. The existing vectorized scatter and geometry
validation policy remain intact.

`test/services/test_geometry.py::test_validation_query_exports_plain_text_geometry_types`
uses the real DuckDB spatial query on small constructed polygon, point and null
inputs. Its RED result was an unsigned dictionary instead of plain text. The
first cast-only repair exposed the existing missing-input sorting contract;
filtering distinct names to present rows restored it. Geometry, GeoParquet and
planning acquisition checks then passed together: 82 tests.

Keep the query boundary independent of Arrow dictionary-to-pandas behavior.
Do not weaken missing/invalid geometry handling, dictionary-encode WKB, or add
a Python per-row geometry fallback to repair this interoperability issue.

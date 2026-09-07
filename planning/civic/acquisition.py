"""Restartable, fail-closed ArcGIS acquisition for civic planning layers.

The collector keeps transport responses short lived.  It freezes the OBJECTID
inventory, writes one transformed parquet per bounded range, and records the
file hash only after the file is complete.  A canonical file is therefore
assembled only from a complete, revalidated checkpoint.
"""

# Runtime import intentionally precedes Polars; this file suppresses import-sort's
# conflicting preference because native thread caps are load-order sensitive.
# ruff: noqa: I001

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import inspect
import json
import logging
import os
import re
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Runtime caps must precede Polars/native imports in this memory-heavy entry point.
import services.runtime_env as _runtime_env  # noqa: F401

import polars as pl

from services.parquet_io import save_parquet

LOG = logging.getLogger(__name__)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_DB_NAME = "acquisition.sqlite3"


class AcquisitionError(RuntimeError):
    """The source or checkpoint failed a publication-critical contract."""


@dataclass(frozen=True)
class LayerResult:
    layer_name: str
    layer_url: str
    complete: bool
    page_paths: tuple[Path, ...]
    expected_count: int
    fetched_count: int
    retained_count: int
    reused_pages: int
    fetched_pages: int
    geometry_reasons: dict[str, int]
    consistency_limitations: tuple[str, ...]
    identity_hash: str


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _ensure_response(response: Any, context: str) -> dict[str, Any]:
    if not isinstance(response, dict):
        raise AcquisitionError(f"{context}: response is not an object")
    if response.get("error"):
        raise AcquisitionError(f"{context}: ArcGIS error: {response['error']}")
    return response


def _transfer_limited(response: dict[str, Any]) -> bool:
    if any(response.get(key) is True for key in ("exceededTransferLimit", "transferLimitExceeded", "truncated")):
        return True
    nested = response.get("properties")
    return isinstance(nested, dict) and any(
        nested.get(key) is True for key in ("exceededTransferLimit", "transferLimitExceeded", "truncated")
    )


def _strict_ids(response: dict[str, Any], context: str) -> list[int]:
    if _transfer_limited(response):
        raise AcquisitionError(f"{context}: truncated OBJECTID inventory")
    raw = response.get("objectIds")
    if not isinstance(raw, list):
        raise AcquisitionError(f"{context}: missing objectIds")
    ids: list[int] = []
    for value in raw:
        if isinstance(value, bool) or not isinstance(value, int):
            raise AcquisitionError(f"{context}: non-integer OBJECTID {value!r}")
        ids.append(value)
    if len(ids) != len(set(ids)):
        raise AcquisitionError(f"{context}: duplicate OBJECTID")
    if any(value < 0 for value in ids):
        raise AcquisitionError(f"{context}: negative OBJECTID")
    return sorted(ids)


def _strict_count(response: dict[str, Any], context: str) -> int:
    raw = response.get("count")
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise AcquisitionError(f"{context}: missing or invalid count")
    return raw


def _oid(feature: dict[str, Any], field: str) -> Any:
    attrs = feature.get("attributes")
    if not isinstance(attrs, dict):
        attrs = feature.get("properties")
    if isinstance(attrs, dict) and field in attrs:
        return attrs[field]
    return feature.get("id")


def _strict_feature_ids(features: Any, field: str, expected: list[int], context: str) -> None:
    if not isinstance(features, list):
        raise AcquisitionError(f"{context}: missing features")
    got = [_oid(feature, field) for feature in features if isinstance(feature, dict)]
    if len(got) != len(features):
        raise AcquisitionError(f"{context}: malformed feature")
    if any(isinstance(value, bool) or not isinstance(value, int) for value in got):
        raise AcquisitionError(f"{context}: missing or non-integer returned OBJECTID")
    if len(got) != len(set(got)):
        raise AcquisitionError(f"{context}: duplicate returned OBJECTID")
    if set(got) != set(expected):
        raise AcquisitionError(f"{context}: returned OBJECTID set does not match planned range")


def _default_transform(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(rows, infer_schema_length=None)


class ArcGISStagedCollector:
    """Collect one or more ArcGIS layers into a caller-owned checkpoint.

    ``request`` is deliberately injected.  Its preferred signature is
    ``request(url, params) -> dict``; a keyword-parameter callable is accepted
    as a convenience for the existing ``_query`` fixture seam.
    """

    def __init__(
        self,
        request: Callable[..., dict[str, Any]],
        checkpoint_dir: str | Path,
        *,
        resume: bool = False,
        page_size: int = 2_000,
        max_pages: int | None = None,
    ) -> None:
        if isinstance(page_size, bool) or page_size <= 0:
            raise ValueError("page_size must be positive")
        if max_pages is not None and (isinstance(max_pages, bool) or max_pages <= 0):
            raise ValueError("max_pages must be positive")
        self.request = request
        self.checkpoint_dir = Path(checkpoint_dir)
        self.page_size = page_size
        self.max_pages = max_pages
        self.resume = resume
        db_path = self.checkpoint_dir / _DB_NAME
        if resume and not self.checkpoint_dir.is_dir():
            raise AcquisitionError("resume requires an existing checkpoint directory")
        if resume and not db_path.exists():
            raise AcquisitionError("resume checkpoint ledger is missing")
        if not resume and db_path.exists():
            raise AcquisitionError(f"checkpoint is occupied: {self.checkpoint_dir}")
        if not resume and self.checkpoint_dir.exists() and any(self.checkpoint_dir.iterdir()):
            raise AcquisitionError(f"checkpoint is occupied: {self.checkpoint_dir}")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.pages_dir = self.checkpoint_dir / "pages"
        self.pages_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(db_path, timeout=0.25)
        self.db.execute("PRAGMA busy_timeout=250")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS layers (
                layer_name TEXT PRIMARY KEY,
                layer_url TEXT NOT NULL,
                identity_json TEXT NOT NULL,
                identity_hash TEXT NOT NULL,
                ids_json TEXT NOT NULL,
                expected_count INTEGER NOT NULL,
                object_id_field TEXT NOT NULL,
                complete INTEGER NOT NULL DEFAULT 0,
                limitations_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pages (
                layer_name TEXT NOT NULL,
                page_no INTEGER NOT NULL,
                low_id INTEGER NOT NULL,
                high_id INTEGER NOT NULL,
                ids_json TEXT NOT NULL,
                path TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                source_count INTEGER NOT NULL,
                fetched_count INTEGER NOT NULL,
                geometry_reasons_json TEXT NOT NULL,
                PRIMARY KEY(layer_name, page_no),
                FOREIGN KEY(layer_name) REFERENCES layers(layer_name)
            );
            """
        )
        self.db.commit()
        self.lock_db = sqlite3.connect(self.checkpoint_dir / "collector.lock.sqlite3", timeout=0.25)
        self.lock_db.execute("PRAGMA busy_timeout=250")
        try:
            self.lock_db.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            self.lock_db.close()
            self.db.close()
            raise AcquisitionError("checkpoint is busy; another collector owns this checkpoint") from exc

    def close(self) -> None:
        if self.db is None:
            return
        try:
            self.lock_db.rollback()
            self.lock_db.close()
        except sqlite3.Error:
            pass
        self.db.close()
        self.db = None

    def __enter__(self) -> ArcGISStagedCollector:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def _call(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        try:
            signature = inspect.signature(self.request)
            parameters = list(signature.parameters.values())
        except (TypeError, ValueError):
            parameters = []
        if len(parameters) >= 2 and parameters[1].kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            return _ensure_response(self.request(url, params), url)
        return _ensure_response(self.request(url, **params), url)

    def _metadata(self, url: str) -> dict[str, Any]:
        response = self._call(url, {"f": "json"})
        field = response.get("objectIdField") or response.get("objectIdFieldName")
        if not isinstance(field, str) or not _IDENTIFIER.fullmatch(field):
            raise AcquisitionError(f"{url}: missing safe objectIdField")
        fields = response.get("fields")
        if not isinstance(fields, list):
            raise AcquisitionError(f"{url}: missing fields schema")
        max_count = response.get("maxRecordCount")
        if isinstance(max_count, bool) or not isinstance(max_count, int) or max_count <= 0:
            raise AcquisitionError(f"{url}: missing maxRecordCount")
        schema = []
        for item in fields:
            if not isinstance(item, dict) or not isinstance(item.get("name"), str):
                raise AcquisitionError(f"{url}: malformed field schema")
            schema.append({key: item.get(key) for key in ("name", "type", "alias", "nullable", "length")})
        edit = response.get("editingInfo")
        edit_token = None
        if isinstance(edit, dict):
            for key in ("lastEditDate", "dataLastEditDate"):
                if edit.get(key) is not None:
                    edit_token = {key: edit[key]}
                    break
        return {
            "object_id_field": field,
            "fields": schema,
            "max_record_count": max_count,
            "editing_info": edit,
            "edit_generation": edit_token,
            "current_version": response.get("currentVersion"),
        }

    def _inventory(self, url: str, where: str, field: str) -> tuple[list[int], int]:
        params = {
            "f": "json",
            "where": where,
            "returnIdsOnly": "true",
            "returnGeometry": "false",
        }
        ids = _strict_ids(self._call(url + "/query", params), f"{url} returnIdsOnly")
        count_response = self._call(
            url + "/query",
            {"f": "json", "where": where, "returnCountOnly": "true", "returnGeometry": "false"},
        )
        count = _strict_count(count_response, f"{url} returnCountOnly")
        if count != len(ids):
            raise AcquisitionError(f"{url}: count {count} does not match ID inventory {len(ids)}")
        return ids, count

    def _identity(
        self,
        url: str,
        where: str,
        layer_name: str,
        metadata: dict[str, Any],
        ids: list[int],
        output_params: dict[str, Any],
        transform_version: str,
        validation_year: int,
    ) -> tuple[str, str]:
        limitations = (
            []
            if metadata["edit_generation"]
            else [
                "No reliable ArcGIS edit-generation token was supplied; read-window and ID reconciliation do not prove an atomic source snapshot."
            ]
        )
        identity = {
            "layer_name": layer_name,
            "source_url": url,
            "where": where,
            "request": output_params,
            "schema": metadata,
            "inventory_sha256": hashlib.sha256(_canonical(ids).encode()).hexdigest(),
            "inventory_count": len(ids),
            "consistency_limitations": limitations,
            "transform_version": transform_version,
            "validation_year": validation_year,
        }
        return _canonical(identity), hashlib.sha256(_canonical(identity).encode()).hexdigest()

    def _begin(self) -> None:
        try:
            self.db.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            raise AcquisitionError("checkpoint is busy; another collector owns this checkpoint") from exc

    def _existing_layer(self, layer_name: str) -> sqlite3.Row | None:
        self.db.row_factory = sqlite3.Row
        return self.db.execute("SELECT * FROM layers WHERE layer_name = ?", (layer_name,)).fetchone()

    def _page_rows(self, layer_name: str) -> list[sqlite3.Row]:
        self.db.row_factory = sqlite3.Row
        return self.db.execute("SELECT * FROM pages WHERE layer_name = ? ORDER BY page_no", (layer_name,)).fetchall()

    def _valid_page(self, row: sqlite3.Row, expected_ids: list[int]) -> Path:
        path = Path(row["path"])
        if not path.is_file() or _sha256(path) != row["sha256"]:
            raise AcquisitionError(f"corrupt staged page: {path}")
        recorded = json.loads(row["ids_json"])
        if recorded != expected_ids:
            raise AcquisitionError(f"incompatible staged page inventory: {path}")
        if int(row["source_count"]) != len(expected_ids):
            raise AcquisitionError(f"incompatible staged page source count: {path}")
        try:
            frame = pl.read_parquet(path)
        except Exception as exc:
            raise AcquisitionError(f"unreadable staged page: {path}") from exc
        if frame.height != int(row["fetched_count"]):
            raise AcquisitionError(f"staged page row count changed: {path}")
        return path

    def collect_layer(
        self,
        layer_url: str,
        *,
        where: str = "1=1",
        layer_name: str,
        transform: Callable[[list[dict[str, Any]]], Any] | None = None,
        adapt_page: Callable[[list[dict[str, Any]], str], Any] | None = None,
        drop_cols: Iterable[str] = (),
        output_params: dict[str, Any] | None = None,
        transform_version: str = "planning-applications-transform-v1",
        validation_year: int | None = None,
    ) -> LayerResult:
        if not layer_name or "/" in layer_name or "\\" in layer_name:
            raise ValueError("layer_name must be a simple checkpoint name")
        transform = transform or _default_transform
        output_params = dict(output_params or {})
        metadata = self._metadata(layer_url)
        oid_field = metadata["object_id_field"]
        if self.page_size > metadata["max_record_count"]:
            raise AcquisitionError(
                f"page_size {self.page_size} exceeds ArcGIS maxRecordCount {metadata['max_record_count']}"
            )
        validation_year = validation_year or dt.date.today().year
        ids, expected_count = self._inventory(layer_url, where, oid_field)
        request_params = {
            "f": output_params.pop("f", "json"),
            "where": where,
            "outFields": output_params.pop("outFields", "*"),
            "returnGeometry": output_params.pop("returnGeometry", "true"),
            "outSR": output_params.pop("outSR", "4326"),
            "resultRecordCount": self.page_size,
            **output_params,
        }
        identity_json, identity_hash = self._identity(
            layer_url, where, layer_name, metadata, ids, request_params, transform_version, validation_year
        )
        old = self._existing_layer(layer_name)
        if old is not None:
            if not self.resume:
                raise AcquisitionError(f"checkpoint layer is occupied: {layer_name}")
            if old["identity_hash"] != identity_hash or old["identity_json"] != identity_json:
                raise AcquisitionError(f"checkpoint identity changed: {layer_name}")
            if json.loads(old["ids_json"]) != ids or int(old["expected_count"]) != expected_count:
                raise AcquisitionError(f"checkpoint ID inventory changed: {layer_name}")
        else:
            self._begin()
            try:
                self.db.execute(
                    "INSERT INTO layers(layer_name, layer_url, identity_json, identity_hash, ids_json, expected_count, object_id_field, limitations_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        layer_name,
                        layer_url,
                        identity_json,
                        identity_hash,
                        _canonical(ids),
                        expected_count,
                        oid_field,
                        _canonical(
                            []
                            if metadata["edit_generation"]
                            else [
                                "No reliable ArcGIS edit-generation token was supplied; read-window and ID reconciliation do not prove an atomic source snapshot."
                            ]
                        ),
                    ),
                )
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

        existing = {int(row["page_no"]): row for row in self._page_rows(layer_name)}
        page_specs = [
            (page_no, ids[offset : min(offset + self.page_size, len(ids))])
            for page_no, offset in enumerate(range(0, len(ids), self.page_size))
        ]
        paths: list[Path] = []
        reused = fetched = fetched_count = retained_count_total = 0
        reasons: dict[str, int] = {}
        budget_seen = 0
        try:
            for page_no, planned_ids in page_specs:
                low, high = planned_ids[0], planned_ids[-1]
                old_page = existing.get(page_no)
                if old_page is not None:
                    path = self._valid_page(old_page, planned_ids)
                    paths.append(path)
                    reused += 1
                    fetched_count += int(old_page["source_count"])
                    retained_count_total += int(old_page["fetched_count"])
                    for reason, count in json.loads(old_page["geometry_reasons_json"]).items():
                        reasons[reason] = reasons.get(reason, 0) + int(count)
                    continue
                if self.max_pages is not None and budget_seen >= self.max_pages:
                    break
                budget_seen += 1
                query_where = f"({where}) AND {oid_field} >= {low} AND {oid_field} <= {high}"
                params = dict(request_params)
                params["where"] = query_where
                path = self.pages_dir / layer_name / f"page-{page_no:06d}.parquet"
                path.parent.mkdir(parents=True, exist_ok=True)
                self._begin()
                try:
                    response = self._call(layer_url + "/query", params)
                    if _transfer_limited(response):
                        raise AcquisitionError(f"{layer_name} page {page_no}: truncated response")
                    features = response.get("features")
                    _strict_feature_ids(features, oid_field, planned_ids, f"{layer_name} page {page_no}")
                    by_id = {_oid(feature, oid_field): feature for feature in features}
                    features = [by_id[object_id] for object_id in planned_ids]
                    if adapt_page is None:
                        rows = []
                        for feature in features:
                            attrs = dict(feature.get("attributes") or feature.get("properties") or {})
                            attrs.pop(oid_field, None)
                            rows.append(attrs)
                        page_reasons: dict[str, int] = {}
                    else:
                        adapted = adapt_page(features, oid_field)
                        if isinstance(adapted, tuple) and len(adapted) == 2:
                            rows, page_reasons = adapted
                        else:
                            rows, page_reasons = adapted, {}
                    for row in rows:
                        if isinstance(row, dict):
                            row.pop(oid_field, None)
                            for column in drop_cols:
                                row.pop(column, None)
                    transformed = transform(rows)
                    if not isinstance(transformed, (pl.DataFrame, pl.LazyFrame)):
                        transformed = pl.DataFrame(transformed, infer_schema_length=None)
                    if isinstance(transformed, pl.LazyFrame):
                        transformed = transformed.collect()
                    retained_count = transformed.height
                    save_parquet(transformed, path)
                    digest = _sha256(path)
                    self.db.execute(
                        "INSERT INTO pages(layer_name, page_no, low_id, high_id, ids_json, path, sha256, source_count, fetched_count, geometry_reasons_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            layer_name,
                            page_no,
                            low,
                            high,
                            _canonical(planned_ids),
                            str(path),
                            digest,
                            len(features),
                            retained_count,
                            _canonical(page_reasons),
                        ),
                    )
                    self.db.commit()
                except BaseException:
                    self.db.rollback()
                    raise
                paths.append(path)
                fetched += 1
                fetched_count += len(features)
                retained_count_total += retained_count
                for reason, count in page_reasons.items():
                    reasons[reason] = reasons.get(reason, 0) + int(count)
            # Supplying a budget always creates a resumable checkpoint.  Even when
            # the cap happens to cover every page, the caller must explicitly rerun
            # uncapped before any canonical path may be replaced.
            complete = len(paths) == len(page_specs) and self.max_pages is None
            if complete:
                self._revalidate(layer_url, where, layer_name, identity_hash, ids, metadata)
                self._begin()
                try:
                    self.db.execute("UPDATE layers SET complete = 1 WHERE layer_name = ?", (layer_name,))
                    self.db.commit()
                except BaseException:
                    self.db.rollback()
                    raise
            return LayerResult(
                layer_name,
                layer_url,
                complete,
                tuple(paths),
                expected_count,
                fetched_count,
                retained_count_total,
                reused,
                fetched,
                reasons,
                tuple(json.loads(self._existing_layer(layer_name)["limitations_json"])),
                identity_hash,
            )
        except AcquisitionError:
            raise
        except Exception as exc:
            raise AcquisitionError(f"{layer_name}: acquisition failed: {exc}") from exc

    def _revalidate(
        self,
        url: str,
        where: str,
        layer_name: str,
        identity_hash: str,
        old_ids: list[int],
        old_metadata: dict[str, Any],
    ) -> None:
        metadata = self._metadata(url)
        ids, count = self._inventory(url, where, old_metadata["object_id_field"])
        if metadata != old_metadata or ids != old_ids or count != len(old_ids):
            raise AcquisitionError(f"{layer_name}: source metadata or ID inventory changed before publication")

    def revalidate_layers(self, results: Iterable[LayerResult]) -> None:
        """Reconcile every requested layer immediately before publication."""
        for result in results:
            row = self._existing_layer(result.layer_name)
            if row is None:
                raise AcquisitionError(f"missing checkpoint layer: {result.layer_name}")
            identity = json.loads(row["identity_json"])
            metadata = self._metadata(result.layer_url)
            ids, count = self._inventory(result.layer_url, identity["where"], identity["schema"]["object_id_field"])
            current_json, current_hash = self._identity(
                result.layer_url,
                identity["where"],
                result.layer_name,
                metadata,
                ids,
                identity["request"],
                identity.get("transform_version", "planning-applications-transform-v1"),
                int(identity.get("validation_year", dt.date.today().year)),
            )
            if (
                current_hash != row["identity_hash"]
                or current_json != row["identity_json"]
                or count != result.expected_count
            ):
                raise AcquisitionError(f"{result.layer_name}: source changed before publication")

    def staged_stats(self, result: LayerResult) -> tuple[int, dict[str, int]]:
        """Return bounded-memory row and authority counts for publication guards."""
        if not result.complete:
            raise AcquisitionError(f"cannot inspect incomplete layer: {result.layer_name}")
        if not result.page_paths:
            return 0, {}
        scans = pl.concat([pl.scan_parquet(path) for path in result.page_paths], how="diagonal_relaxed")
        row_count = int(scans.select(pl.len()).collect().item())
        if "PlanningAuthority" not in scans.collect_schema().names():
            return row_count, {}
        grouped = scans.group_by("PlanningAuthority").len().collect()
        counts = {str(row["PlanningAuthority"]): int(row["len"]) for row in grouped.iter_rows(named=True)}
        return row_count, counts

    def assemble(
        self, result: LayerResult, destination: str | Path, *, min_rows: int | None = None, **kwargs: Any
    ) -> Path:
        if not result.complete:
            raise AcquisitionError(f"cannot assemble incomplete layer: {result.layer_name}")
        if not result.page_paths:
            frame = pl.DataFrame()
        else:
            frame = pl.concat([pl.scan_parquet(path) for path in result.page_paths], how="diagonal_relaxed")
        return save_parquet(frame, destination, min_rows=min_rows, **kwargs)


__all__ = ["AcquisitionError", "ArcGISStagedCollector", "LayerResult"]


@contextlib.contextmanager
def publication_lock(path: str | Path):
    """Serialize canonical publication across independent checkpoint dirs."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise AcquisitionError(f"canonical publication is busy: {path}") from exc
    try:
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(str(os.getpid()))
        yield path
    finally:
        with contextlib.suppress(OSError):
            path.unlink()


__all__.append("publication_lock")

"""Local, known-item retrieval pilot over source-linked council-minute passages.

Reads an explicit parquet path or synthetic JSON/JSONL. Dense retrieval is opt-in
and requires pre-cached pinned model weights; corpus and queries stay local.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

SCHEMA_VERSION = 1
JUDGMENT_STATUS = "agent_authored_seed_requires_human_review"
_MAX_CORPUS_ROWS = 20_000
_MAX_TEXT_BYTES = 64 * 1024 * 1024
_MAX_JSON_BYTES = 128 * 1024 * 1024
_REVISION = re.compile(r"^[0-9a-fA-F]{40}$")
_ID_FIELDS = ("document_id", "docID", "doc_id")
_URL_FIELDS = ("source_url", "sourceURL")
_OCR_FIELDS = ("ocr_status", "OCRstatus", "ocrStatus")
_STATUS_FIELDS = ("source_status", "sourceStatus")
_DATE_FIELDS = ("date", "meeting_date", "meetingDate")
_TEXT_FIELDS = ("body", "text")
_REQUIRED_QUERY_FIELDS = {
    "id",
    "kind",
    "query",
    "document_id",
    "chunk",
    "body_sha256",
    "evidence_quote",
}


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _first(record: dict[str, Any], fields: tuple[str, ...], label: str) -> Any:
    for field in fields:
        if field in record:
            return record[field]
    raise ValueError(f"corpus row missing {label}")


def _canonical_row(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"corpus row {index} must be an object")
    document_id = _first(raw, _ID_FIELDS, "document_id")
    source_url = _first(raw, _URL_FIELDS, "source_url")
    body = _first(raw, _TEXT_FIELDS, "body")
    try:
        chunk = int(raw["chunk"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"corpus row {index} has invalid chunk") from exc
    if not isinstance(document_id, str) or not document_id.strip():
        raise ValueError(f"corpus row {index} has empty document_id")
    if not isinstance(body, str) or not body.strip():
        raise ValueError(f"corpus row {index} has empty body")
    if (
        not isinstance(source_url, str)
        or urlsplit(source_url).scheme not in {"http", "https"}
        or not urlsplit(source_url).netloc
    ):
        raise ValueError(f"corpus row {index} source_url must be an HTTP(S) URL")
    if "council" not in raw:
        raise ValueError(f"corpus row {index} missing council")
    date = _first(raw, _DATE_FIELDS, "date")
    date = "" if date is None else str(date)
    try:
        ocr_status = _first(raw, _OCR_FIELDS, "ocr_status")
    except ValueError:
        ocr_status = _first(raw, _STATUS_FIELDS, "source_status")
    if not isinstance(ocr_status, str) or not ocr_status.strip():
        raise ValueError(f"corpus row {index} has empty ocr_status")
    return {
        "document_id": document_id,
        "chunk": chunk,
        "body": body,
        "source_url": source_url,
        "council": raw["council"],
        "date": date,
        "meeting_date": date,
        "ocr_status": ocr_status,
        "source_status": ocr_status,
    }


def _read_records(path: Path, *, query_file: bool = False) -> Any:
    try:
        if path.stat().st_size > _MAX_JSON_BYTES:
            raise ValueError(f"JSON input exceeds {_MAX_JSON_BYTES} byte limit: {path}")
        raw = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"cannot read {path}: {exc}") from exc
    if not raw.strip():
        raise ValueError(f"{path} is empty")
    try:
        parsed = json.loads(raw.decode("utf-8"))
        return parsed
    except (UnicodeDecodeError, json.JSONDecodeError):
        if query_file:
            raise ValueError(f"query file must be JSON: {path}") from None
    records = []
    for line_no, line in enumerate(raw.decode("utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL at {path}:{line_no}") from exc
    return records


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _check_corpus_bounds(row_count: int, text_bytes: int) -> None:
    if row_count > _MAX_CORPUS_ROWS:
        raise ValueError(f"corpus row limit {_MAX_CORPUS_ROWS} exceeded: {row_count}")
    if text_bytes > _MAX_TEXT_BYTES:
        raise ValueError(f"corpus text byte limit {_MAX_TEXT_BYTES} exceeded: {text_bytes}")


def _load_parquet(path: Path, memory_limit: str = "512MB") -> list[dict[str, Any]]:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("parquet corpus loading requires the cached duckdb package") from exc
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET threads=1")
        con.execute("SET memory_limit = ?", [memory_limit])
        schema = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        names = {str(row[0]).lower(): str(row[0]) for row in schema}

        def column(fields: tuple[str, ...], label: str) -> str:
            for field in fields:
                if field.lower() in names:
                    return names[field.lower()]
            raise ValueError(f"parquet corpus missing {label}")

        selected = {
            "document_id": column(_ID_FIELDS, "document_id"),
            "source_url": column(_URL_FIELDS, "source_url"),
            "body": column(_TEXT_FIELDS, "body"),
            "chunk": column(("chunk",), "chunk"),
            "council": column(("council",), "council"),
            "date": column(_DATE_FIELDS, "date"),
            "ocr_status": column(_OCR_FIELDS + _STATUS_FIELDS, "ocr_status/source_status"),
        }
        select_sql = ", ".join(
            f"{_quoted_identifier(actual)} AS {_quoted_identifier(alias)}" for alias, actual in selected.items()
        )
        url_col = _quoted_identifier(selected["source_url"])
        body_col = _quoted_identifier(selected["body"])
        where = (
            f"WHERE regexp_matches(CAST({url_col} AS VARCHAR), '^https?://') "
            f"AND {body_col} IS NOT NULL AND length(trim(CAST({body_col} AS VARCHAR))) > 0"
        )
        counts = con.execute(
            f"SELECT count(*), coalesce(sum(octet_length(encode(CAST({body_col} AS VARCHAR)))), 0) "
            f"FROM read_parquet(?) {where}",
            [str(path)],
        ).fetchone()
        assert counts is not None
        row_count, text_bytes = counts
        _check_corpus_bounds(row_count, text_bytes)
        rows = con.execute(
            f"SELECT {select_sql} FROM read_parquet(?) {where}",
            [str(path)],
        ).fetchall()
        assert con.description is not None
        columns = [item[0] for item in con.description]
        return [dict(zip(columns, row, strict=True)) for row in rows]
    finally:
        con.close()


def load_corpus(path: Path, memory_limit: str = "512MB") -> list[dict[str, Any]]:
    """Load and validate source-linked rows, returning deterministic row order."""
    parsed = _load_parquet(path, memory_limit) if path.suffix.lower() == ".parquet" else _read_records(path)
    if isinstance(parsed, dict):
        parsed = parsed.get("documents", parsed.get("rows"))
    if not isinstance(parsed, list):
        raise ValueError("corpus must be a parquet, JSON array, JSONL file, or object with documents/rows")
    _check_corpus_bounds(len(parsed), 0)
    rows = [_canonical_row(row, i) for i, row in enumerate(parsed, 1)]
    _check_corpus_bounds(len(rows), sum(len(row["body"].encode("utf-8")) for row in rows))
    seen: set[tuple[str, int]] = set()
    for row in rows:
        key = (row["document_id"], row["chunk"])
        if key in seen:
            raise ValueError(f"duplicate document_id/chunk in corpus: {key[0]}/{key[1]}")
        seen.add(key)
    if not rows:
        raise ValueError("corpus has no rows")
    return sorted(rows, key=lambda row: (row["document_id"], row["chunk"]))


def _canonical_query(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"query {index} must be an object")
    missing = _REQUIRED_QUERY_FIELDS - raw.keys()
    if missing:
        raise ValueError(f"query {index} missing required fields: {', '.join(sorted(missing))}")
    out = dict(raw)
    if not isinstance(out["id"], str) or not out["id"].strip():
        raise ValueError(f"query {index} has empty id")
    if not isinstance(out["query"], str) or not out["query"].strip():
        raise ValueError(f"query {index} has empty query")
    if not isinstance(out["kind"], str) or not out["kind"].strip():
        raise ValueError(f"query {index} has empty kind")
    try:
        out["chunk"] = int(out["chunk"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"query {index} has invalid chunk") from exc
    if not isinstance(out["body_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", out["body_sha256"]):
        raise ValueError(f"query {index} body_sha256 must be a lowercase SHA-256")
    if not isinstance(out["evidence_quote"], str) or not out["evidence_quote"]:
        raise ValueError(f"query {index} evidence_quote must be nonempty")
    return out


def load_queries(path: Path, rows: list[dict[str, Any]], *, corpus_sha256: str | None = None) -> list[dict[str, Any]]:
    parsed = _read_records(path, query_file=True)
    if not isinstance(parsed, dict) or parsed.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("query schema_version must be 1")
    if parsed.get("judgment_status") != JUDGMENT_STATUS:
        raise ValueError("query judgment_status must be agent_authored_seed_requires_human_review")
    if "corpus_sha256_at_selection" in parsed and parsed["corpus_sha256_at_selection"] != corpus_sha256:
        raise ValueError("corpus_sha256_at_selection does not match the current corpus")
    raw_queries = parsed.get("queries")
    if not isinstance(raw_queries, list) or not raw_queries:
        raise ValueError("queries must be a nonempty array")
    queries = [_canonical_query(query, i) for i, query in enumerate(raw_queries, 1)]
    ids = [query["id"] for query in queries]
    if len(set(ids)) != len(ids):
        raise ValueError("query ids must be unique")
    by_key = {(row["document_id"], row["chunk"]): row for row in rows}
    for query in queries:
        key = (query["document_id"], query["chunk"])
        target = by_key.get(key)
        if target is None:
            raise ValueError(f"query {query['id']} target does not exist: {key}")
        if query["body_sha256"] != _sha256_text(target["body"]):
            raise ValueError(f"query {query['id']} body_sha256 does not match target body")
        if query["evidence_quote"] not in target["body"]:
            raise ValueError(f"query {query['id']} evidence_quote is not an exact target substring")
        for field in ("source_url", "council", "meeting_date", "source_status"):
            if field in query and query[field] != target[field]:
                raise ValueError(f"query {query['id']} {field} does not match target metadata")
    return queries


def _hit_key(hit: Any) -> tuple[str, int]:
    if isinstance(hit, dict):
        return (str(hit["document_id"]), int(hit["chunk"]))
    if isinstance(hit, (tuple, list)) and len(hit) >= 2:
        return str(hit[0]), int(hit[1]) if isinstance(hit[1], int) else 0
    value = str(hit)
    if "#" in value:
        document_id, chunk = value.rsplit("#", 1)
        try:
            return document_id, int(chunk)
        except ValueError:
            pass
    return value, 0


def metrics_for_rankings(
    queries: list[dict[str, Any]], rows: list[dict[str, Any]], rankings: dict[str, Any]
) -> dict[str, dict[str, float | int]]:
    """Compute only known-target metrics, independently for every ranking arm."""
    del rows
    result: dict[str, dict[str, float | int]] = {}
    for arm, ranking in rankings.items():
        hits_at = {1: 0, 5: 0, 10: 0}
        reciprocal = 0.0
        for query in queries:
            if isinstance(ranking, dict):
                ordered = ranking.get(query["id"], [])
            else:
                ordered = ranking if len(queries) == 1 else []
            target = (query["document_id"], int(query["chunk"]))
            rank = None
            seen: set[tuple[str, int]] = set()
            position = 0
            for hit in ordered:
                key = _hit_key(hit)
                if key in seen:
                    continue
                seen.add(key)
                position += 1
                if key == target:
                    rank = position
                    break
            if rank is not None:
                for cutoff in hits_at:
                    hits_at[cutoff] += rank <= cutoff
                if rank <= 10:
                    reciprocal += 1.0 / rank
        denominator = max(1, len(queries))
        result[arm] = {
            "known_target_hit_at_1": hits_at[1],
            "known_target_hit_at_5": hits_at[5],
            "known_target_hit_at_10": hits_at[10],
            "reciprocal_rank_at_10": round(reciprocal / denominator, 6),
            "known_target_query_count": len(queries),
        }
    return result


def _result_id(row: dict[str, Any]) -> str:
    return f"{row['document_id']}#{row['chunk']}"


def _ranked_output(
    query: dict[str, Any], ordered: list[dict[str, Any]], rows: dict[tuple[str, int], dict[str, Any]]
) -> dict[str, Any]:
    hits = []
    seen: set[tuple[str, int]] = set()
    for item in ordered:
        key = (item["document_id"], int(item["chunk"]))
        if key in seen:
            continue
        seen.add(key)
        row = rows[key]
        hits.append(
            {
                "id": _result_id(row),
                "document_id": row["document_id"],
                "chunk": row["chunk"],
                "score": round(float(item["score"]), 9),
                "source_url": row["source_url"],
                "council": row["council"],
                "date": row["date"],
                "meeting_date": row.get("meeting_date", row["date"]),
                "ocr_status": row["ocr_status"],
                "source_status": row.get("source_status", row["ocr_status"]),
            }
        )
        if len(hits) >= 10:
            break
    target_id = _result_id(query)
    target_rank = next((index for index, hit in enumerate(hits, 1) if hit["id"] == target_id), None)
    return {
        "query_id": query["id"],
        "kind": query["kind"],
        "query": query["query"],
        "known_target_id": target_id,
        "known_target_rank_at_10": target_rank,
        "top10": hits,
    }


def _sort_scored(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: (-float(item["score"]), item["document_id"], int(item["chunk"])))


def _bm25(
    rows: list[dict[str, Any]], queries: list[dict[str, Any]], memory_limit: str = "512MB"
) -> dict[str, list[dict[str, Any]]]:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("BM25 requires the cached duckdb package") from exc
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET threads=1")
        con.execute("SET memory_limit = ?", [memory_limit])
        con.execute("LOAD fts")
        con.execute("CREATE TABLE docs (rid BIGINT, document_id VARCHAR, chunk INTEGER, body VARCHAR)")
        con.executemany(
            "INSERT INTO docs VALUES (?, ?, ?, ?)",
            [(i, row["document_id"], row["chunk"], row["body"]) for i, row in enumerate(rows)],
        )
        con.execute("PRAGMA create_fts_index('docs', 'rid', 'body', ignore='[^a-z0-9]+')")
        out: dict[str, list[dict[str, Any]]] = {}
        for query in queries:
            records = con.execute(
                "SELECT document_id, chunk, fts_main_docs.match_bm25(rid, ?) AS score "
                "FROM docs WHERE score IS NOT NULL ORDER BY score DESC, document_id, chunk LIMIT 50",
                [query["query"]],
            ).fetchall()
            out[query["id"]] = [
                {"document_id": record[0], "chunk": int(record[1]), "score": float(record[2])} for record in records
            ]
        return out
    finally:
        con.close()


def _validate_matrix(matrix: Any, expected_rows: int, label: str) -> None:
    import numpy as np

    if matrix.ndim != 2 or matrix.shape[0] != expected_rows or matrix.shape[1] == 0:
        raise RuntimeError(f"{label} embedding matrix has invalid shape")
    norms = np.linalg.norm(matrix, axis=1)
    if not np.isfinite(matrix).all() or (norms == 0).any():
        raise RuntimeError(f"{label} embedding matrix has non-finite or zero vectors")
    if not np.allclose(norms, 1.0, rtol=1e-3, atol=1e-3):
        raise RuntimeError(f"{label} embedding vectors must be normalized")


def _cache_paths(cache_dir: Path, label: str) -> tuple[Path, Path]:
    return cache_dir / f"council_{label}.npz", cache_dir / f"council_{label}.manifest.json"


def _cache_load(matrix_path: Path, manifest_path: Path, manifest: dict[str, Any], expected_rows: int) -> Any | None:
    try:
        stored = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(stored, dict):
            return None
        matrix_sha = stored.pop("matrix_sha256", None)
        if stored != manifest or matrix_sha != file_sha256(matrix_path):
            return None
        import numpy as np

        with np.load(matrix_path, allow_pickle=False) as loaded:
            matrix = loaded["vectors"]
        _validate_matrix(matrix, expected_rows, "cached")
        return matrix
    except (OSError, ValueError, KeyError, RuntimeError, json.JSONDecodeError):
        return None


def _cache_save(cache_dir: Path, label: str, matrix: Any, manifest: dict[str, Any]) -> None:
    import numpy as np

    cache_dir.mkdir(parents=True, exist_ok=True)
    matrix_path, manifest_path = _cache_paths(cache_dir, label)
    with tempfile.NamedTemporaryFile(dir=cache_dir, prefix=f".{label}.", suffix=".npz", delete=False) as tmp:
        matrix_tmp = Path(tmp.name)
    manifest_tmp = matrix_tmp.with_suffix(".manifest.json")
    try:
        np.savez_compressed(matrix_tmp, vectors=matrix)
        manifest_tmp.write_text(
            json.dumps({**manifest, "matrix_sha256": file_sha256(matrix_tmp)}, sort_keys=True), encoding="utf-8"
        )
        os.replace(matrix_tmp, matrix_path)
        os.replace(manifest_tmp, manifest_path)
    finally:
        matrix_tmp.unlink(missing_ok=True)
        manifest_tmp.unlink(missing_ok=True)


def _encode_batches(
    model: Any, texts: list[str], batch_size: int, label: str, dense_backend: str = "sentence-transformers"
) -> Any:
    import numpy as np

    parts = []
    _tick(f"embedding {label} 0/{len(texts)}")
    for start in range(0, len(texts), 16):
        stop = min(start + 16, len(texts))
        options = {"batch_size": batch_size, "show_progress_bar": False}
        if dense_backend == "model2vec":
            # Its default is 512 tokens and also clips characters before tokenizing.
            options.update(max_length=None, use_multiprocessing=False)
        else:
            options.update(normalize_embeddings=True, convert_to_numpy=True)
        vectors = np.asarray(model.encode(texts[start:stop], **options), dtype=np.float32)
        _validate_matrix(vectors, stop - start, label)
        parts.append(vectors)
        _tick(f"embedding {label} {stop}/{len(texts)}")
    return np.concatenate(parts, axis=0)


def _dense(
    rows: list[dict[str, Any]],
    queries: list[dict[str, Any]],
    *,
    cache_dir: Path | None,
    model_name: str,
    revision: str,
    device: str,
    batch_size: int,
    max_length: int,
    corpus_sha: str,
    query_sha: str,
    dense_backend: str = "sentence-transformers",
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    if not _REVISION.fullmatch(revision):
        raise ValueError("--revision must be an explicit 40-character hexadecimal revision")
    try:
        import numpy as np
    except ImportError as exc:
        raise RuntimeError("dense arm requires numpy") from exc
    if dense_backend == "model2vec":
        if device != "cpu":
            raise ValueError("model2vec requires --device cpu")
        try:
            # Optional packages are installed only in the isolated pilot runtime.
            from huggingface_hub import snapshot_download  # pyright: ignore[reportMissingImports]
            from model2vec import StaticModel  # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            raise RuntimeError("model2vec arm requires model2vec and huggingface-hub") from exc
        local = Path(
            snapshot_download(
                model_name,
                revision=revision,
                cache_dir=str(cache_dir) if cache_dir else None,
                local_files_only=True,
                token=False,
            )
        )
        for name in ("model.safetensors", "tokenizer.json", "config.json"):
            if not (local / name).is_file():
                raise RuntimeError(f"missing local model file: {name}")
        model = StaticModel.from_pretrained(local, force_download=False, normalize=True)
        dtype = "float32"
    elif dense_backend == "sentence-transformers":
        try:
            import torch  # pyright: ignore[reportMissingImports]
            from sentence_transformers import SentenceTransformer  # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            raise RuntimeError("dense arm requires sentence-transformers and torch") from exc
        if device == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("dense arm requested cuda but torch.cuda.is_available() is false")
        dtype = "float16" if device == "cuda" else "float32"
        model = SentenceTransformer(
            model_name,
            revision=revision,
            cache_folder=str(cache_dir) if cache_dir else None,
            device=device,
            trust_remote_code=False,
            local_files_only=True,
            model_kwargs={"torch_dtype": torch.float16 if device == "cuda" else torch.float32},
        )
        if max_length > model.max_seq_length:
            raise ValueError(f"--max-length {max_length} exceeds model limit {model.max_seq_length}")
        model.max_seq_length = max_length
    else:
        raise ValueError(f"unknown dense backend: {dense_backend}")
    texts = [row["body"] for row in rows]
    query_texts = [query["query"] for query in queries]
    token_counts = {}
    for label, values in (("document", texts), ("query", query_texts)):
        lengths = []
        for i, value in enumerate(values, 1):
            if dense_backend == "model2vec":
                ids = model.tokenize([value], max_length=None)[0]
            else:
                ids = model.tokenizer(value, add_special_tokens=True, truncation=False, return_attention_mask=False)[
                    "input_ids"
                ]
            if len(ids) > max_length:
                raise ValueError(f"{label} {i} has {len(ids)} tokens, exceeding --max-length {max_length}")
            lengths.append(len(ids))
        token_counts[label] = {"total": sum(lengths), "max": max(lengths)}
    common = {
        "model_name": model_name,
        "revision": revision,
        "max_length": max_length,
        "device": device,
        "dtype": dtype,
        "backend": dense_backend,
        "normalize_embeddings": True,
        "batch_size": batch_size,
        "packages": _packages(),
        "source_script_sha256": file_sha256(Path(__file__)),
    }
    ordered_hashes = [{"id": _result_id(row), "body_sha256": _sha256_text(row["body"])} for row in rows]
    doc_manifest = {
        "kind": "documents",
        "corpus_sha256": corpus_sha,
        "ordered_ids_text_hashes": ordered_hashes,
        **common,
    }
    query_manifest = {
        "kind": "queries",
        "query_file_sha256": query_sha,
        "ordered_query_hashes": [{"id": q["id"], "query_sha256": _sha256_text(q["query"])} for q in queries],
        **common,
    }
    doc_vectors = _cache_load(*_cache_paths(cache_dir, "documents"), doc_manifest, len(rows)) if cache_dir else None
    query_vectors = (
        _cache_load(*_cache_paths(cache_dir, "queries"), query_manifest, len(queries)) if cache_dir else None
    )
    cache_hits = {"documents": doc_vectors is not None, "queries": query_vectors is not None}
    if doc_vectors is None:
        doc_vectors = _encode_batches(model, texts, batch_size, "documents", dense_backend)
        _validate_matrix(doc_vectors, len(rows), "document")
        if cache_dir:
            _cache_save(cache_dir, "documents", doc_vectors, doc_manifest)
    if query_vectors is None:
        query_vectors = _encode_batches(model, query_texts, batch_size, "queries", dense_backend)
        _validate_matrix(query_vectors, len(queries), "query")
        if cache_dir:
            _cache_save(cache_dir, "queries", query_vectors, query_manifest)
    scores = np.matmul(query_vectors, doc_vectors.T)
    out = {}
    for qi, query in enumerate(queries):
        order = np.argsort(-scores[qi], kind="stable")
        ranked = [
            {
                "document_id": rows[int(di)]["document_id"],
                "chunk": rows[int(di)]["chunk"],
                "score": float(scores[qi, int(di)]),
            }
            for di in order[:50]
        ]
        out[query["id"]] = _sort_scored(ranked)
    return out, {
        "backend": dense_backend,
        "model": model_name,
        "revision": revision,
        "device": device,
        "dtype": dtype,
        "max_length": max_length,
        "batch_size": batch_size,
        "normalize_embeddings": True,
        "cache_hits": cache_hits,
        "encoder_token_counts": token_counts,
        "encoder_token_policy": (
            "no special tokens; unknown tokens removed; max_length=None (no truncation)"
            if dense_backend == "model2vec"
            else "special tokens included; no truncation"
        ),
    }


def _rrf(
    bm25: dict[str, list[dict[str, Any]]], dense: dict[str, list[dict[str, Any]]]
) -> dict[str, list[dict[str, Any]]]:
    out = {}
    for query_id in sorted(set(bm25) | set(dense)):
        combined: dict[tuple[str, int], dict[str, Any]] = {}
        for ranking in (bm25.get(query_id, [])[:50], dense.get(query_id, [])[:50]):
            for rank, hit in enumerate(ranking, 1):
                key = (hit["document_id"], int(hit["chunk"]))
                combined.setdefault(key, {**hit, "score": 0.0})["score"] += 1.0 / (60 + rank)
        out[query_id] = _sort_scored(list(combined.values()))[:50]
    return out


def _git_sha() -> str:
    repo = Path(__file__).resolve().parents[2]
    try:
        return subprocess.check_output(
            ["git", "-C", str(repo), "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("could not determine source git SHA") from exc


def _memory_check(min_free_mb: int) -> dict[str, int]:
    if min_free_mb <= 0:
        raise ValueError("--min-free-mb must be positive")
    if platform.system() != "Windows":
        return {}
    from tools.hooks.guard_memory import sample_memory

    memory = sample_memory()
    free = memory.get("free_mb")
    if free is not None and free < min_free_mb:
        raise RuntimeError(f"memory guard: {free} MB free is below --min-free-mb {min_free_mb}")
    commit_free = memory.get("commit_free_mb")
    if commit_free is not None and commit_free < min_free_mb:
        raise RuntimeError(f"memory guard: {commit_free} MB commit available is below --min-free-mb {min_free_mb}")
    return memory


def _packages() -> dict[str, str]:
    names = (
        "duckdb",
        "numpy",
        "sentence-transformers",
        "torch",
        "transformers",
        "tokenizers",
        "huggingface-hub",
        "model2vec",
    )
    out = {}
    for name in names:
        try:
            out[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            out[name] = "unavailable"
    return out


def run(args: argparse.Namespace) -> dict[str, Any]:
    # This is deliberately the first project import.  Heavy third-party imports are lazy.
    import services.runtime_env  # noqa: F401

    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    started = time.perf_counter()
    if Path(args.output).exists():
        raise FileExistsError(f"refusing to overwrite existing output: {args.output}")
    if args.backend == "all" and (not args.revision or not _REVISION.fullmatch(args.revision)):
        raise ValueError("--revision must be an explicit 40-character hexadecimal revision")
    corpus_path = Path(args.corpus)
    query_path = Path(args.queries)
    memory = _memory_check(args.min_free_mb)
    rows = load_corpus(corpus_path, args.memory_limit)
    corpus_sha = file_sha256(corpus_path)
    queries = load_queries(query_path, rows, corpus_sha256=corpus_sha)
    query_sha = file_sha256(query_path)
    _tick(f"loaded corpus {len(rows)}/{len(rows)} rows")
    timings = {"corpus_and_judgments": time.perf_counter() - started}
    arm_started = time.perf_counter()
    rankings: dict[str, dict[str, list[dict[str, Any]]]] = {"bm25": _bm25(rows, queries, args.memory_limit)}
    timings["bm25_build_and_query"] = time.perf_counter() - arm_started
    model_info: dict[str, Any] = {"backend": "none", "fp16": False, "revision": None}
    if args.backend == "all":
        _memory_check(args.min_free_mb)
        arm_started = time.perf_counter()
        dense, dense_info = _dense(
            rows,
            queries,
            cache_dir=Path(args.cache_dir) if args.cache_dir else None,
            model_name=args.model,
            revision=args.revision,
            device=args.device,
            batch_size=args.batch_size,
            max_length=args.max_length,
            corpus_sha=corpus_sha,
            query_sha=query_sha,
            dense_backend=args.dense_backend,
        )
        rankings["dense"] = dense
        timings["dense_load_tokenize_encode_and_score"] = time.perf_counter() - arm_started
        arm_started = time.perf_counter()
        rankings["rrf"] = _rrf(rankings["bm25"], dense)
        timings["rrf"] = time.perf_counter() - arm_started
        model_info = {**dense_info, "fp16": dense_info["dtype"] == "float16"}
    rows_by_key = {(row["document_id"], row["chunk"]): row for row in rows}
    metric_rows = metrics_for_rankings(queries, rows, rankings)
    arms = {}
    for arm, ranking in rankings.items():
        arms[arm] = {
            **metric_rows[arm],
            "metrics_by_kind": {
                kind: metrics_for_rankings([query for query in queries if query["kind"] == kind], rows, {arm: ranking})[
                    arm
                ]
                for kind in sorted({query["kind"] for query in queries})
            },
            "queries": [_ranked_output(query, ranking.get(query["id"], []), rows_by_key) for query in queries],
        }
    report = {
        "schema_version": SCHEMA_VERSION,
        "experiment": "council_retrieval_pilot",
        "corpus_sha256": corpus_sha,
        "query_file_sha256": query_sha,
        "source_git_sha": _git_sha(),
        "source_script_sha256": file_sha256(Path(__file__)),
        "source_worktree_dirty": bool(
            subprocess.check_output(
                ["git", "-C", str(Path(__file__).resolve().parents[2]), "status", "--porcelain"], text=True
            ).strip()
        ),
        "corpus_row_count": len(rows),
        "query_count": len(queries),
        "backend_requested": args.backend,
        "model_backend": model_info,
        "arms": arms,
        "wall_timings_seconds": {
            **{key: round(value, 6) for key, value in timings.items()},
            "total": round(time.perf_counter() - started, 6),
        },
        "configuration": {
            "memory_limit": args.memory_limit,
            "min_free_mb": args.min_free_mb,
            "max_corpus_rows": _MAX_CORPUS_ROWS,
            "max_corpus_text_bytes": _MAX_TEXT_BYTES,
            "bm25": {
                "ignore": "[^a-z0-9]+",
                "other_tokenizer_options": "DuckDB FTS defaults",
                "threads": 1,
                "top_k": 50,
            },
            "rrf": {"k": 60, "input_top_k": 50},
        },
        "packages": _packages(),
        "memory_observation": memory,
        "warnings": [
            "Judgments are agent-authored seeds and require human review.",
            "Judgments are partial and do not represent exhaustive relevance labels.",
        ],
        "token_economics": "not measured: no downstream LLM",
    }
    return report


def _tick(message: str) -> None:
    print(message, flush=True)


def _atomic_output(path: Path, report: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing output: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    ) as handle:
        temp = Path(handle.name)
        json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    try:
        if path.exists():
            raise FileExistsError(f"refusing to overwrite existing output: {path}")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an offline known-item council-minutes retrieval pilot.")
    parser.add_argument("--corpus", required=True, type=Path, help="source-linked corpus parquet, JSON, or JSONL")
    parser.add_argument("--queries", required=True, type=Path, help="version-1 authored judgment JSON")
    parser.add_argument("--output", required=True, type=Path, help="new JSON report path; existing files are refused")
    parser.add_argument("--backend", choices=("bm25", "all"), default="bm25")
    parser.add_argument(
        "--dense-backend", choices=("sentence-transformers", "model2vec"), default="sentence-transformers"
    )
    parser.add_argument("--cache-dir", type=Path, default=None)
    parser.add_argument("--revision", default=None, help="40-hex model revision, required for --backend all")
    parser.add_argument("--model", default="BAAI/bge-m3")
    parser.add_argument("--device", choices=("cpu", "cuda"), default="cuda")
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--max-length", type=int, default=1024)
    parser.add_argument("--memory-limit", default="512MB", help="DuckDB memory limit")
    parser.add_argument("--min-free-mb", type=int, default=1536)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.batch_size != 1:
        parser.error("--batch-size must be 1 for this pilot")
    if args.max_length <= 0:
        parser.error("--max-length must be positive")
    if args.backend == "all" and not args.revision:
        parser.error("--revision is required for --backend all")
    try:
        report = run(args)
        _atomic_output(args.output, report)
    except (OSError, RuntimeError, ValueError) as exc:
        parser.exit(2, f"council retrieval pilot failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

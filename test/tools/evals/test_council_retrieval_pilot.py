from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.evals import council_retrieval_pilot as pilot


def _body_sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_metrics_are_separate_per_arm_and_stale_or_duplicate_inputs_fail(tmp_path):
    body = "The council approved the river flood scheme."
    corpus = tmp_path / "corpus.jsonl"
    corpus.write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {
                    "document_id": "doc-a",
                    "chunk": 0,
                    "body": body,
                    "source_url": "https://example.test/a",
                    "council": "River",
                    "date": "2026-01-02",
                    "ocr_status": "born_digital",
                },
                {
                    "document_id": "doc-b",
                    "chunk": 0,
                    "body": "The council discussed housing repairs.",
                    "source_url": "https://example.test/b",
                    "council": "River",
                    "date": "2026-01-03",
                    "ocr_status": "born_digital",
                },
            ]
        ),
        encoding="utf-8",
    )
    rows = pilot.load_corpus(corpus)
    query_file = tmp_path / "queries.json"
    query_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "judgment_status": "agent_authored_seed_requires_human_review",
                "queries": [
                    {
                        "id": "q1",
                        "kind": "known_positive",
                        "query": "river flood scheme",
                        "document_id": "doc-a",
                        "chunk": 0,
                        "body_sha256": _body_sha(body),
                        "evidence_quote": "approved the river flood scheme",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    queries = pilot.load_queries(query_file, rows)

    report = pilot.metrics_for_rankings(
        queries,
        rows,
        {
            "bm25": [("doc-a", 1.0), ("doc-b", 0.5)],
            "dense": [("doc-b", 0.9), ("doc-a", 0.8)],
        },
    )
    assert report["bm25"]["known_target_hit_at_1"] == 1
    assert report["dense"]["known_target_hit_at_1"] == 0
    assert report["dense"]["known_target_hit_at_5"] == 1
    assert report["dense"]["reciprocal_rank_at_10"] == 0.5

    duplicate = tmp_path / "duplicate.jsonl"
    duplicate.write_text(
        corpus.read_text(encoding="utf-8") + "\n" + corpus.read_text(encoding="utf-8"), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="duplicate.*document_id.*chunk"):
        pilot.load_corpus(duplicate)

    stale = json.loads(query_file.read_text(encoding="utf-8"))
    stale["queries"][0]["body_sha256"] = "0" * 64
    stale_path = tmp_path / "stale.json"
    stale_path.write_text(json.dumps(stale), encoding="utf-8")
    with pytest.raises(ValueError, match="body_sha256"):
        pilot.load_queries(stale_path, rows)


def test_parquet_loader_filters_unlinked_rows_and_keeps_council_metadata(tmp_path, monkeypatch):
    duckdb = pytest.importorskip("duckdb")
    parquet = tmp_path / "synthetic.parquet"
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            "CREATE TABLE source AS SELECT * FROM (VALUES "
            "('doc-a', 0, 'River', DATE '2026-01-02', 'born_digital', "
            "'https://example.test/a', 'The river flood scheme was approved.'), "
            "('doc-b', 0, 'River', DATE '2026-01-03', 'ocr_winocr', 'file:///private/b', 'unlinked'), "
            "('doc-c', 0, 'River', DATE '2026-01-04', 'ocr_winocr', 'https://example.test/c', '   ') "
            ") AS t(document_id, chunk, council, meeting_date, source_status, source_url, body)"
        )
        con.execute("COPY source TO ? (FORMAT PARQUET)", [str(parquet)])
    finally:
        con.close()

    rows = pilot.load_corpus(parquet)
    assert [(row["document_id"], row["meeting_date"], row["source_status"]) for row in rows] == [
        ("doc-a", "2026-01-02", "born_digital")
    ]
    monkeypatch.setattr(pilot, "_MAX_CORPUS_ROWS", 0)
    with pytest.raises(ValueError, match="row limit"):
        pilot.load_corpus(parquet)


def test_dense_batches_report_measured_progress_and_validate_unit_vectors(capsys):
    import numpy as np

    class Encoder:
        def encode(self, texts, **kwargs):
            assert kwargs["normalize_embeddings"] is True
            assert kwargs["batch_size"] == 1
            return np.tile([1.0, 0.0], (len(texts), 1))

    vectors = pilot._encode_batches(Encoder(), ["synthetic"] * 33, 1, "documents")
    assert vectors.shape == (33, 2)
    progress = capsys.readouterr().out
    assert "embedding documents 16/33" in progress
    assert "embedding documents 32/33" in progress
    assert "embedding documents 33/33" in progress
    with pytest.raises(RuntimeError, match="normalized"):
        pilot._validate_matrix(np.array([[2.0, 0.0]]), 1, "cached")


def test_cache_rejects_changed_matrix_even_when_shape_and_norms_match(tmp_path):
    import numpy as np

    manifest = {"revision": "a" * 40, "corpus_sha256": "b" * 64}
    vectors = np.array([[1.0, 0.0]])
    pilot._cache_save(tmp_path, "documents", vectors, manifest)
    matrix_path, manifest_path = pilot._cache_paths(tmp_path, "documents")
    loaded = pilot._cache_load(matrix_path, manifest_path, manifest, 1)
    assert loaded is not None
    assert np.array_equal(loaded, vectors)
    np.savez_compressed(matrix_path, vectors=np.array([[0.0, 1.0]]))
    assert pilot._cache_load(matrix_path, manifest_path, manifest, 1) is None
    manifest_path.write_text("[]", encoding="utf-8")
    assert pilot._cache_load(matrix_path, manifest_path, manifest, 1) is None


def test_rrf_fuses_ranks_with_stable_ties_and_one_hit_per_passage():
    def hit(doc, chunk=0):
        return {"document_id": doc, "chunk": chunk, "score": 0.9}

    ranking = pilot._rrf({"q": [hit("a"), hit("b")]}, {"q": [hit("b"), hit("a")]})["q"]
    assert [item["document_id"] for item in ranking] == ["a", "b"]
    assert ranking[0]["score"] == pytest.approx(1 / 61 + 1 / 62)


def test_query_provenance_rejects_corpus_and_declared_metadata_drift(tmp_path):
    body = "The river flood scheme was approved."
    row = {
        "document_id": "doc-a",
        "chunk": 2,
        "body": body,
        "source_url": "https://example.test/a",
        "council": "River",
        "meeting_date": "2026-01-02",
        "source_status": "text",
    }
    corpus = tmp_path / "corpus.json"
    corpus.write_text(json.dumps([row]), encoding="utf-8")
    rows = pilot.load_corpus(corpus)
    actual_sha = pilot.file_sha256(corpus)
    query = {
        "id": "q1",
        "kind": "natural",
        "query": "river flood approval",
        "document_id": "doc-a",
        "chunk": 2,
        "body_sha256": _body_sha(body),
        "evidence_quote": "flood scheme",
        "source_url": row["source_url"],
        "council": "River",
        "meeting_date": "2026-01-02",
        "source_status": "text",
    }
    payload = {
        "schema_version": 1,
        "judgment_status": pilot.JUDGMENT_STATUS,
        "corpus_sha256_at_selection": actual_sha,
        "queries": [query],
    }
    path = tmp_path / "queries.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert pilot.load_queries(path, rows, corpus_sha256=actual_sha)[0]["chunk"] == 2
    with pytest.raises(ValueError, match="corpus_sha256_at_selection"):
        pilot.load_queries(path, rows, corpus_sha256="0" * 64)
    query["source_url"] = "https://example.test/different"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="source_url"):
        pilot.load_queries(path, rows, corpus_sha256=actual_sha)


def test_partial_metrics_use_deduplicated_nonzero_chunk_ranks():
    query = {"id": "q", "document_id": "b", "chunk": 7}
    hit_a = {"document_id": "a", "chunk": 3, "score": 1.0}
    hit_b = {"document_id": "b", "chunk": 7, "score": 0.5}
    result = pilot.metrics_for_rankings([query], [], {"dense": {"q": [hit_a, hit_a, hit_b]}})
    assert result["dense"]["reciprocal_rank_at_10"] == 0.5


def test_dense_refuses_truncation_and_requires_local_pinned_model(monkeypatch):
    options = {}

    class Model:
        max_seq_length = 8192

        def __init__(self, model_name, **kwargs):
            options.update(kwargs)

        def tokenizer(self, *args, **kwargs):
            assert kwargs["truncation"] is False
            return {"input_ids": list(range(4097))}

        def encode(self, *args, **kwargs):
            pytest.fail("over-limit passage reached encoder")

    monkeypatch.setitem(
        sys.modules,
        "torch",
        SimpleNamespace(cuda=SimpleNamespace(is_available=lambda: True), float16="fp16", float32="fp32"),
    )
    monkeypatch.setitem(sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=Model))
    with pytest.raises(ValueError, match="4097 tokens.*max-length 2048"):
        pilot._dense(
            [{"body": "synthetic"}],
            [{"query": "synthetic"}],
            cache_dir=None,
            model_name="BAAI/bge-m3",
            revision="a" * 40,
            device="cuda",
            batch_size=1,
            max_length=2048,
            corpus_sha="b" * 64,
            query_sha="c" * 64,
        )
    assert options["revision"] == "a" * 40
    assert options["local_files_only"] is True
    assert options["trust_remote_code"] is False


def test_model2vec_cpu_uses_pinned_local_files_and_all_tokens(tmp_path, monkeypatch):
    import numpy as np

    calls = {}
    snapshot = tmp_path / ("a" * 40)
    snapshot.mkdir()
    for name in ("model.safetensors", "tokenizer.json", "config.json"):
        (snapshot / name).write_text("synthetic", encoding="utf-8")

    def local_snapshot(model_name, **kwargs):
        calls["snapshot"] = kwargs
        return str(snapshot)

    class StaticModel:
        @classmethod
        def from_pretrained(cls, path, **kwargs):
            assert Path(path) == snapshot
            assert kwargs == {"force_download": False, "normalize": True}
            return cls()

        def tokenize(self, texts, max_length):
            assert max_length is None
            return [list(range(601)) for _ in texts]

        def encode(self, texts, **kwargs):
            assert kwargs == {
                "batch_size": 1,
                "max_length": None,
                "show_progress_bar": False,
                "use_multiprocessing": False,
            }
            calls["encoded"] = calls.get("encoded", 0) + len(texts)
            return np.asarray([[1.0, 0.0] for _ in texts])

    monkeypatch.setitem(sys.modules, "huggingface_hub", SimpleNamespace(snapshot_download=local_snapshot))
    monkeypatch.setitem(sys.modules, "model2vec", SimpleNamespace(StaticModel=StaticModel))
    rows = [{"document_id": "a", "chunk": 0, "body": "synthetic long text"}]
    queries = [{"id": "q", "query": "synthetic long query"}]
    rankings, info = pilot._dense(
        rows,
        queries,
        cache_dir=None,
        model_name="minishlab/potion-base-8M",
        revision="a" * 40,
        device="cpu",
        batch_size=1,
        max_length=1024,
        corpus_sha="b" * 64,
        query_sha="c" * 64,
        dense_backend="model2vec",
    )
    assert calls["snapshot"]["local_files_only"] is True
    assert calls["snapshot"]["revision"] == "a" * 40
    assert calls["snapshot"]["token"] is False
    assert calls["encoded"] == 2
    assert rankings["q"][0]["score"] == 1.0
    assert info["backend"] == "model2vec"
    assert info["encoder_token_counts"]["document"]["max"] == 601
    (snapshot / "tokenizer.json").unlink()
    with pytest.raises(RuntimeError, match="missing local model file"):
        pilot._dense(
            rows,
            queries,
            cache_dir=None,
            model_name="minishlab/potion-base-8M",
            revision="a" * 40,
            device="cpu",
            batch_size=1,
            max_length=1024,
            corpus_sha="b" * 64,
            query_sha="c" * 64,
            dense_backend="model2vec",
        )

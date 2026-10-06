from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))

from tools.evals.app_retrieval_recall import evaluate_project_recall  # noqa: E402


def test_recall_uses_exact_normalized_paths_and_separate_surfaces():
    report = evaluate_project_recall(
        [
            {
                "query": "gold",
                "gold": "./SRC/gold.py",
                "result": {
                    "results": [
                        {"path": "SRC/sibling.py", "why": "SRC/gold.py appears in this explanation"},
                        {"path": "SRC\\gold.py"},
                    ],
                    "content_spans": [
                        {"path": "./SRC/gold.py", "span": "10-12"},
                        {"path": "SRC/sibling.py", "span": "1-100"},
                    ],
                },
                "expected_span": "10-12",
            },
            {
                "query": "content only",
                "gold": "src/content.py",
                "result": {"hits": [], "content_spans": [{"path": "src/content.py", "span": "4-7"}]},
                "expected_span": "4-7",
            },
        ]
    )

    assert report["eligible"] == 2
    assert report["excluded"] == 0
    assert report["metadata"]["ranks"] == [2, None]
    assert report["content_spans"]["ranks"] == [1, 1]
    assert report["metadata"]["r1"] == 0
    assert report["metadata"]["r5"] == 0.5
    assert report["metadata"]["mrr"] == 0.25
    assert report["either_surface"]["r1"] == 1
    assert report["union"]["ordering"] == "metadata-first, then content_spans; deduplicated by normalized path"
    assert report["union"]["ranks"] == [2, 1]
    assert report["cases"] == [
        {
            "query": "gold",
            "gold": "SRC/gold.py",
            "eligible": True,
            "reason": None,
            "expected_span_invalid": False,
            "metadata_rank": 2,
            "content_rank": 1,
            "union_rank": 2,
            "either_rank": 1,
        },
        {
            "query": "content only",
            "gold": "src/content.py",
            "eligible": True,
            "reason": None,
            "expected_span_invalid": False,
            "metadata_rank": None,
            "content_rank": 1,
            "union_rank": 1,
            "either_rank": 1,
        },
    ]


def test_recall_malformed_spans_are_nonmatches_and_private_gold_is_ineligible():
    report = evaluate_project_recall(
        [
            {
                "query": "bad span",
                "gold": "src/gold.py",
                "result": {
                    "results": [{"path": "src/gold.py", "span": "not-a-span"}],
                    "content_spans": [{"path": "src/gold.py", "span": "12-10"}],
                },
                "expected_span": "3-4",
            },
            {
                "query": "private",
                "gold": "planning/product/mcp/precedent_fts.py",
                "result": {"results": [{"path": "planning/product/mcp/precedent_fts.py"}]},
            },
        ]
    )

    assert report["eligible"] == 1
    assert report["excluded"] == 1
    assert report["excluded_paths"] == ["planning/product/mcp/precedent_fts.py"]
    assert report["metadata"]["ranks"] == [1]
    assert report["content_spans"]["ranks"] == [None]
    assert report["union"]["ranks"] == [1]


def test_empty_recall_denominators_are_unknown():
    report = evaluate_project_recall([])

    for surface in ("metadata", "content_spans", "union", "either_surface"):
        assert report[surface]["denominator"] == 0
        assert report[surface]["r1"] is None
        assert report[surface]["r5"] is None
        assert report[surface]["mrr"] is None


def test_content_span_requires_valid_expected_coverage_but_metadata_path_is_separate():
    report = evaluate_project_recall(
        [
            {
                "gold": "src/gold.py",
                "result": {
                    "results": [{"path": "src/gold.py"}],
                    "content_spans": [{"path": "src/gold.py"}],
                },
                "expected_span": "3-4",
            },
            {
                "gold": "src/other.py",
                "result": {"content_spans": [{"path": "src/other.py", "span": "3-4"}]},
                "expected_span": "unknown",
            },
        ]
    )

    assert report["metadata"]["ranks"] == [1, None]
    assert report["content_spans"]["ranks"] == [None, None]
    assert report["span_expected_invalid"] == 1


def test_legacy_bare_list_is_metadata_only_and_invalid_golds_are_excluded():
    report = evaluate_project_recall(
        [
            {"gold": "src/gold.py", "result": [{"path": "src/gold.py", "span": "1-2"}]},
            {"gold": None, "result": {}},
            {"gold": "", "result": {}},
            {"gold": "C:/outside.py", "result": {}},
            {"gold": "planning/product/mcp/precedent_fts.py", "result": {}},
        ]
    )

    assert report["total"] == report["eligible"] + report["excluded"]
    assert report["eligible"] == 1
    assert report["excluded"] == 4
    assert report["metadata"]["ranks"] == [1]
    assert report["content_spans"]["ranks"] == [None]
    assert len(report["excluded_reasons"]) == 4
    assert report["cases"][-1]["eligible"] is False
    assert report["cases"][-1]["reason"] == "private tree excluded from public scan policy"


@pytest.mark.parametrize(
    ("span", "expected_span"),
    [
        (None, "3-4"),
        ("not-a-span", "3-4"),
        ("1-2", "3-4"),
        (None, None),
        ("not-a-span", None),
        ("1-2", None),
    ],
)
def test_content_only_union_requires_content_origin_span_validation(span, expected_span):
    case = {
        "gold": "src/content.py",
        "result": {"content_spans": [{"path": "src/content.py", **({"span": span} if span is not None else {})}]},
    }
    if expected_span is not None:
        case["expected_span"] = expected_span

    report = evaluate_project_recall([case])

    expected_rank = 1 if span is not None and span == "1-2" and expected_span is None else None
    assert report["metadata"]["ranks"] == [None]
    assert report["content_spans"]["ranks"] == [expected_rank]
    assert report["union"]["ranks"] == [expected_rank]

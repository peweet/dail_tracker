"""Native validation is for decoded wire JSON; legacy Python-object semantics remain."""

import json
from decimal import Decimal
from pathlib import Path

import pytest
import responses
from hypothesis import given, settings
from hypothesis import strategies as st

from services import http_engine, schema_validation

WRAPPERS = {
    "members": "member",
    "legislation": "bill",
    "questions": "question",
    "votes": "division",
    "debates": "debateRecord",
}


def _page(wrapper="member"):
    return {"head": {"counts": {"resultCount": 1}}, "results": [{wrapper: {"source": "authored fixture"}}]}


@pytest.mark.parametrize("endpoint", WRAPPERS)
@responses.activate
def test_fetch_valid_wire_json_does_not_run_the_python_validator(monkeypatch, endpoint):
    url = f"https://api.oireachtas.ie/v1/{endpoint}?limit=1"
    fixture = Path(__file__).resolve().parents[1] / "fixtures/api" / f"{endpoint}_sample.json"
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    responses.add(responses.GET, url, json=payload, status=200)

    def slow_path(*args, **kwargs):
        pytest.fail("valid wire JSON still reached the Python validation path")

    monkeypatch.setattr(schema_validation, "validate_response", slow_path)
    result, count = http_engine.fetch_json(url)
    assert result == payload
    assert count > 0


@pytest.mark.parametrize("mutation", ["tuple", "decimal"])
def test_direct_python_object_validation_keeps_existing_type_rules(mutation):
    payload = _page()
    if mutation == "tuple":
        payload["results"] = tuple(payload["results"])
    else:
        payload["head"]["counts"]["resultCount"] = Decimal("1.0")
    with pytest.raises(schema_validation.SchemaValidationError):
        schema_validation.validate_response("members", payload)


@pytest.mark.parametrize("endpoint,wrapper", WRAPPERS.items())
@pytest.mark.parametrize(
    "mutation", ["valid", "missing_head", "bad_count", "missing_results", "missing_wrapper", "bad_wrapper", "additive"]
)
def test_native_wire_validation_preserves_acceptance_and_exact_errors(endpoint, wrapper, mutation):
    payload = _page(wrapper)
    if mutation == "missing_head":
        del payload["head"]
    elif mutation == "bad_count":
        payload["head"]["counts"]["resultCount"] = True
    elif mutation == "missing_results":
        del payload["results"]
    elif mutation == "missing_wrapper":
        payload["results"] = [{"renamed": {}}]
    elif mutation == "bad_wrapper":
        payload["results"] = [{wrapper: None}]
    elif mutation == "additive":
        payload["extra"] = {"unicode": "Dún", "nested": [None, False, 1.25, {"more": "accepted"}]}
    _assert_wire_parity(endpoint, json.loads(json.dumps(payload)))


def _assert_wire_parity(endpoint, payload):
    expected = None
    try:
        schema_validation.validate_response(endpoint, payload)
    except schema_validation.SchemaValidationError as error:
        expected = (str(error), error.endpoint, type(error.__cause__))
    url = f"https://api.oireachtas.ie/v1/{endpoint}"
    if expected is None:
        assert schema_validation.validate_decoded_json_if_known(url, payload) is None
    else:
        with pytest.raises(schema_validation.SchemaValidationError) as caught:
            schema_validation.validate_decoded_json_if_known(url, payload)
        error = caught.value
        assert (str(error), error.endpoint, type(error.__cause__)) == expected


@pytest.mark.parametrize(
    "raw",
    [
        '{"head":{"counts":{"resultCount":"bad","resultCount":1}},"results":[]}',
        '{"head":{"counts":{"resultCount":1,"resultCount":"bad"}},"results":[]}',
        '{"head":{"counts":{"resultCount":NaN}},"results":[]}',
        '{"head":{"counts":{"resultCount":Infinity}},"results":[]}',
        '{"head":{"counts":{"resultCount":1}},"results":[],"extra":[NaN,Infinity,-Infinity]}',
        '{"head":{"counts":{"resultCount":1}},"results":[],"extra":"\\ud800"}',
    ],
)
def test_decoded_duplicate_and_nonfinite_values_keep_legacy_behavior(raw):
    _assert_wire_parity("members", json.loads(raw))


@pytest.mark.parametrize("endpoint,wrapper", WRAPPERS.items())
@settings(max_examples=50, deadline=None)
@given(
    count=st.one_of(
        st.integers(min_value=-(10**100), max_value=10**100),
        st.floats(),
        st.booleans(),
        st.none(),
        st.text(max_size=16),
    )
)
def test_generated_decoded_counts_match_python_semantics(endpoint, wrapper, count):
    payload = _page(wrapper)
    payload["head"]["counts"]["resultCount"] = count
    _assert_wire_parity(endpoint, json.loads(json.dumps(payload)))


def test_native_validators_are_cached_by_endpoint(monkeypatch):
    import jsonschema_rs

    original = jsonschema_rs.validator_for
    compiled = []

    def compile_schema(schema):
        compiled.append(schema)
        return original(schema)

    schema_validation._native_validator.cache_clear()
    monkeypatch.setattr(jsonschema_rs, "validator_for", compile_schema)
    try:
        for endpoint, wrapper in WRAPPERS.items():
            for _ in range(2):
                schema_validation.validate_decoded_json_if_known(
                    f"https://api.oireachtas.ie/v1/{endpoint}", _page(wrapper)
                )
        assert len(compiled) == len(WRAPPERS)
    finally:
        schema_validation._native_validator.cache_clear()


def test_unknown_hosts_and_endpoints_do_not_compile_a_native_validator(monkeypatch):
    def unexpected(endpoint):
        pytest.fail("unknown endpoint compiled a validator")

    monkeypatch.setattr(schema_validation, "_native_validator", unexpected)
    schema_validation.validate_decoded_json_if_known("https://example.com/v1/members", {})
    schema_validation.validate_decoded_json_if_known("https://api.oireachtas.ie/v1/unknown", {})


@pytest.mark.parametrize("endpoint,wrapper", WRAPPERS.items())
@responses.activate
def test_post_response_uses_the_native_success_path(monkeypatch, endpoint, wrapper):
    url = f"https://api.oireachtas.ie/v1/{endpoint}"
    responses.add(responses.POST, url, json=_page(wrapper), status=200)

    def unexpected(*args, **kwargs):
        pytest.fail("valid POST response reached the Python validator")

    monkeypatch.setattr(schema_validation, "validate_response", unexpected)
    payload, size = http_engine.post_json(url, {"request": "fixture"})
    assert payload == _page(wrapper)
    assert size > 0

"""Unit tests for the API endpoint canaries in pdf_infra/pdf_endpoint_check.

No network: each canary is driven with a fake session whose get/post returns a
canned response. Locks the contract — a canary is OK only on HTTP 200 + the
expected response shape, and BROKEN on non-200, wrong shape, or transport error.
This is the source-health validation for the feed/API sources (Oireachtas API,
lobbying.ie, TED, eTenders, Wikidata) that can't be HEAD-checked.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pdf_infra.pdf_endpoint_check import (  # noqa: E402
    canary_etenders,
    canary_lobbying,
    canary_oireachtas_api,
    canary_ted,
    canary_wikidata,
)


class _Resp:
    def __init__(self, status=200, json_data=None, content=b"", text=""):
        self.status_code = status
        self._json = json_data
        self.content = content
        self.text = text

    def json(self):
        return self._json


class _Session:
    """Fake session: returns one canned response for get/post (or raises)."""

    def __init__(self, resp=None, raises=None):
        self._resp = resp
        self._raises = raises
        self.calls = []

    def get(self, url, **kw):
        self.calls.append(("get", url, kw))
        if self._raises:
            raise self._raises
        return self._resp

    def post(self, url, **kw):
        self.calls.append(("post", url, kw))
        if self._raises:
            raise self._raises
        return self._resp


# ── Oireachtas API ────────────────────────────────────────────────────────────
def test_oireachtas_api_ok():
    sess = _Session(_Resp(200, {"head": {"counts": {"resultCount": 1}}, "results": [{"x": 1}]}))
    r = canary_oireachtas_api(sess)
    assert r["ok"] and r["http_status"] == 200 and r["rows"] == 1


def test_oireachtas_api_bad_shape_fails():
    assert not canary_oireachtas_api(_Session(_Resp(200, {"unexpected": True})))["ok"]


def test_oireachtas_api_non_200_fails():
    r = canary_oireachtas_api(_Session(_Resp(503, {})))
    assert not r["ok"] and r["http_status"] == 503


def test_oireachtas_api_transport_error_fails():
    r = canary_oireachtas_api(_Session(raises=ConnectionError("boom")))
    assert not r["ok"] and r["http_status"] is None


# ── lobbying.ie ───────────────────────────────────────────────────────────────
def test_lobbying_ok():
    csv = b"Id,Lobbyist Name,Date Published,Period,Relevant Matter,Extra\nrow1\n"
    r = canary_lobbying(_Session(_Resp(200, content=csv)))
    assert r["ok"] and r["rows"] == 1


def test_lobbying_missing_columns_fails():
    assert not canary_lobbying(_Session(_Resp(200, content=b"Id,Wrong\n")))["ok"]


# ── TED ───────────────────────────────────────────────────────────────────────
def test_ted_ok_uses_post():
    sess = _Session(_Resp(200, {"notices": [{"publication-number": "x"}]}))
    r = canary_ted(sess)
    assert r["ok"] and r["rows"] == 1
    assert sess.calls[0][0] == "post"


def test_ted_bad_shape_fails():
    assert not canary_ted(_Session(_Resp(200, {"oops": 1})))["ok"]


# ── eTenders (CKAN) ───────────────────────────────────────────────────────────
def test_etenders_ok():
    body = {"success": True, "result": {"resources": [{"url": "a"}, {"url": "b"}]}}
    r = canary_etenders(_Session(_Resp(200, body)))
    assert r["ok"] and r["rows"] == 2


def test_etenders_no_resources_fails():
    body = {"success": True, "result": {"resources": []}}
    assert not canary_etenders(_Session(_Resp(200, body)))["ok"]


# ── Wikidata SPARQL ───────────────────────────────────────────────────────────
def test_wikidata_ok():
    r = canary_wikidata(_Session(_Resp(200, {"results": {"bindings": [{"x": {}}]}})))
    assert r["ok"] and r["rows"] == 1


def test_wikidata_bad_shape_fails():
    assert not canary_wikidata(_Session(_Resp(200, {"results": {}})))["ok"]

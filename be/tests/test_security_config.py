"""Tests for the session-key, CORS, and table-cache hardening.

Each test states the weakness it pins down; all three were live defects.
"""

import importlib

import pytest
from fastapi import HTTPException


# --- #4: session signing key -------------------------------------------------

def _reload_config(monkeypatch, **env):
    import app.config as config

    for key in ("SECRET_KEY", "PUBLIC_AUTH_ENABLED", "PUBLIC_REDIRECT_URI",
                "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(config)


def test_no_hardcoded_secret_key_fallback(monkeypatch):
    """The old code signed sessions with a constant committed to a public repo."""
    cfg = _reload_config(monkeypatch, PUBLIC_AUTH_ENABLED="false")
    assert cfg.SECRET_KEY != "@#dsfdds1112"
    assert len(cfg.SECRET_KEY) >= 32


def test_auth_enabled_without_secret_key_refuses_to_start(monkeypatch):
    """Failing closed beats signing sessions with a guessable key."""
    with pytest.raises(RuntimeError, match="SECRET_KEY must be set"):
        _reload_config(monkeypatch, PUBLIC_AUTH_ENABLED="true")


def test_explicit_secret_key_is_used_verbatim(monkeypatch):
    cfg = _reload_config(monkeypatch, PUBLIC_AUTH_ENABLED="true", SECRET_KEY="operator-supplied")
    assert cfg.SECRET_KEY == "operator-supplied"


def test_generated_keys_differ_between_runs(monkeypatch):
    first = _reload_config(monkeypatch, PUBLIC_AUTH_ENABLED="false").SECRET_KEY
    second = _reload_config(monkeypatch, PUBLIC_AUTH_ENABLED="false").SECRET_KEY
    assert first != second, "an ephemeral key must not be reproducible"


# --- #3: CORS ---------------------------------------------------------------

def test_wildcard_origin_is_not_configured_by_default(monkeypatch):
    """allow_origins=['*'] with credentials let any site make authenticated calls."""
    cfg = _reload_config(monkeypatch, PUBLIC_AUTH_ENABLED="false")
    assert "*" not in cfg.CORS_ALLOW_ORIGINS


def test_cors_origins_parsed_from_env(monkeypatch):
    cfg = _reload_config(
        monkeypatch,
        PUBLIC_AUTH_ENABLED="false",
        CORS_ALLOW_ORIGINS="https://a.example.com, https://b.example.com",
    )
    assert cfg.CORS_ALLOW_ORIGINS == ["https://a.example.com", "https://b.example.com"]


def test_cors_defaults_to_redirect_uri_origin(monkeypatch):
    """A sane same-origin default so operators need not think about CORS."""
    cfg = _reload_config(
        monkeypatch,
        PUBLIC_AUTH_ENABLED="false",
        PUBLIC_REDIRECT_URI="https://lakevision.example.com/callback?x=1",
    )
    assert cfg.CORS_ALLOW_ORIGINS == ["https://lakevision.example.com"]


# --- #5: table cache keying and bounds --------------------------------------

class _Req:
    """Minimal stand-in for a Starlette Request."""

    def __init__(self, page_session_id=None, user=None):
        self.headers = {} if page_session_id is None else {"X-Page-Session-ID": page_session_id}
        self.session = {} if user is None else {"user": user}


@pytest.fixture
def deps(monkeypatch):
    import app.dependencies as dependencies

    dependencies.page_session_cache.clear()
    monkeypatch.setattr(dependencies, "load_table", lambda table_id: f"table::{table_id}")
    return dependencies


def test_missing_page_session_header_is_rejected(deps):
    with pytest.raises(HTTPException) as exc:
        deps.get_table(_Req(), "ns.tbl")
    assert exc.value.status_code == 400


def test_same_page_session_id_is_not_shared_between_users(deps, monkeypatch):
    """The ID is client-chosen, so two users could collide and share a Table."""
    monkeypatch.setattr(deps.config, "AUTH_ENABLED", True)
    deps.get_table(_Req("COLLIDING-ID", user="alice@example.com"), "ns.tbl")
    deps.get_table(_Req("COLLIDING-ID", user="bob@example.com"), "ns.tbl")
    users = {key[0] for key in deps.page_session_cache}
    assert users == {"alice@example.com", "bob@example.com"}


def test_cache_hit_avoids_reloading_the_table(deps):
    calls = []
    original = deps.load_table

    def counting(table_id):
        calls.append(table_id)
        return original(table_id)

    deps.load_table = counting
    deps.get_table(_Req("sid"), "ns.tbl")
    deps.get_table(_Req("sid"), "ns.tbl")
    assert len(calls) == 1, "second request should be served from cache"


def test_cache_is_bounded_against_unbounded_growth(deps, monkeypatch):
    """Random session IDs previously grew the dict without limit (memory DoS)."""
    monkeypatch.setattr(deps.config, "PAGE_CACHE_MAX_ENTRIES", 10)
    for i in range(200):
        deps.get_table(_Req(f"attacker-{i}"), "ns.tbl")
    assert len(deps.page_session_cache) <= 10


def test_cache_evicts_least_recently_used_first(deps, monkeypatch):
    monkeypatch.setattr(deps.config, "PAGE_CACHE_MAX_ENTRIES", 3)
    for name in ("a", "b", "c"):
        deps.get_table(_Req(name), "ns.tbl")
    deps.get_table(_Req("a"), "ns.tbl")   # refresh "a"
    deps.get_table(_Req("d"), "ns.tbl")   # evicts "b", the LRU
    remaining = {key[1] for key in deps.page_session_cache}
    assert "a" in remaining and "b" not in remaining

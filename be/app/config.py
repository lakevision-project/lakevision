import logging
import os
import secrets

# --- Authentication & Authorization ---
AUTH_ENABLED = os.getenv("PUBLIC_AUTH_ENABLED", '') == 'true'
CLIENT_ID = os.getenv("PUBLIC_OPENID_CLIENT_ID", '')
OPENID_PROVIDER_URL = os.getenv("PUBLIC_OPENID_PROVIDER_URL", '')
REDIRECT_URI = os.getenv("PUBLIC_REDIRECT_URI", '')
CLIENT_SECRET = os.getenv("OPEN_ID_CLIENT_SECRET", '')
TOKEN_URL = f"{OPENID_PROVIDER_URL}/token"


def _resolve_secret_key() -> str:
    """Resolve the session-signing key.

    A hardcoded fallback was previously used, which meant any deployment that
    forgot to set SECRET_KEY could have its session cookies forged by anyone who
    had read the (public) source. There is no safe constant default, so:

      * with auth enabled, refuse to start rather than sign with a guessable key;
      * otherwise generate a random key, which invalidates sessions on restart --
        the safe failure mode for a local/OSS run without auth.
    """
    configured = os.getenv("SECRET_KEY", "").strip()
    if configured:
        return configured
    if AUTH_ENABLED:
        raise RuntimeError(
            "SECRET_KEY must be set when PUBLIC_AUTH_ENABLED=true. "
            "Generate one with: python -c 'import secrets; print(secrets.token_urlsafe(32))'"
        )
    logging.warning(
        "SECRET_KEY is not set; generating an ephemeral key. Sessions will not "
        "survive a restart. Set SECRET_KEY for any persistent deployment."
    )
    return secrets.token_urlsafe(32)


SECRET_KEY = _resolve_secret_key()

AUTHZ_MODULE = os.getenv("AUTHZ_MODULE_NAME") or "authz"
AUTHZ_CLASS = os.getenv("AUTHZ_CLASS_NAME") or "Authz"

# Auto-prefix with current package if user passes a bare name like "authz"
if "." not in AUTHZ_MODULE:
    # Assumes the authz module is under the app package/folder
    AUTHZ_MODULE = f"app.{AUTHZ_MODULE}"

# --- Cache ---
CACHE_EXPIRATION = 4 * 60  # 4 minutes

# --- CORS ---
# Frontend and backend are served from the same origin via nginx, so no cross
# origin access is needed by default. "*" combined with allow_credentials=True is
# invalid per the CORS spec and makes Starlette echo the caller's origin, letting
# any site issue credentialed, session-authenticated requests (CSRF).
_raw_origins = os.getenv("CORS_ALLOW_ORIGINS", "").strip()
CORS_ALLOW_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]
if not CORS_ALLOW_ORIGINS and REDIRECT_URI:
    from urllib.parse import urlparse as _urlparse

    _parsed = _urlparse(REDIRECT_URI)
    if _parsed.scheme and _parsed.netloc:
        CORS_ALLOW_ORIGINS = [f"{_parsed.scheme}://{_parsed.netloc}"]
if "*" in CORS_ALLOW_ORIGINS:
    logging.warning(
        "CORS_ALLOW_ORIGINS contains '*'; credentialed cross-origin requests will "
        "be disabled to avoid a CSRF hole. List explicit origins instead."
    )

# --- Session cache (#5) ---
# Bounds the per-table cache so a client cannot grow it without limit.
PAGE_CACHE_MAX_ENTRIES = int(os.getenv("PAGE_CACHE_MAX_ENTRIES", "512"))

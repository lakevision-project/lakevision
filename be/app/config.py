import os

# --- Authentication & Authorization ---
AUTH_ENABLED = os.getenv("PUBLIC_AUTH_ENABLED", '') == 'true'
CLIENT_ID = os.getenv("PUBLIC_OPENID_CLIENT_ID", '')
OPENID_PROVIDER_URL = os.getenv("PUBLIC_OPENID_PROVIDER_URL", '')
REDIRECT_URI = os.getenv("PUBLIC_REDIRECT_URI", '')
CLIENT_SECRET = os.getenv("OPEN_ID_CLIENT_SECRET", '')
TOKEN_URL = f"{OPENID_PROVIDER_URL}/token"
SECRET_KEY = os.getenv("SECRET_KEY", "@#dsfdds1112")

AUTHZ_MODULE = os.getenv("AUTHZ_MODULE_NAME") or "authz"
AUTHZ_CLASS = os.getenv("AUTHZ_CLASS_NAME") or "Authz"

# Auto-prefix with current package if user passes a bare name like "authz"
if "." not in AUTHZ_MODULE:
    # Assumes the authz module is under the app package/folder
    AUTHZ_MODULE = f"app.{AUTHZ_MODULE}"

# --- Cache ---
CACHE_EXPIRATION = 4 * 60  # 4 minutes
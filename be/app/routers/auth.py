import requests
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config as AuthlibConfig
from fastapi.responses import JSONResponse

from app import config
from app.models import TokenRequest
from app.dependencies import check_auth

router = APIRouter()

oauth_config = AuthlibConfig(environ={'AUTHLIB_INSECURE_TRANSPORT': '1'})
oauth = OAuth(oauth_config)
oauth.register(
    name="openid",
    client_id=config.CLIENT_ID,
    client_secret=config.CLIENT_SECRET,
    server_metadata_url=f"{config.OPENID_PROVIDER_URL}/.well-known/openid-configuration",
    client_kwargs={"scope": "email"}
)

@router.get("/")
def root(request: Request):
    if config.AUTH_ENABLED:
        user = check_auth(request)
        return {"message": f"You are logged in as {user}"} if user else {"message": "You are not logged in."}
    return {"message": "Hello, authentication is not enabled."}

@router.get("/api/login")
async def login(request: Request):
    return await oauth.openid.authorize_redirect(request, config.REDIRECT_URI)

@router.post("/api/auth/token")
def get_token(request: Request, token_req: TokenRequest):
    """
    Exchanges the SSO code for an Access Token, sets the token in a secure 
    HTTP-only cookie, and returns minimal user details.
    """
    data = {
        "grant_type": "authorization_code",
        "code": token_req.code,
        "client_id": config.CLIENT_ID,
        "client_secret": config.CLIENT_SECRET,
        "redirect_uri": config.REDIRECT_URI,
    }
    response = requests.post(config.TOKEN_URL, data=data)
    
    if response.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to exchange code for token")
    
    token_data = response.json()
    access_token = token_data["access_token"]
    
    user_info_resp = requests.get(
        f"{config.OPENID_PROVIDER_URL}/userinfo",
        headers={'Authorization': f'Bearer {access_token}'}
    )
    user_data = user_info_resp.json()
    
    user_return_data = {
        "id": user_data.get('sub', user_data.get('email', 'unknown')), 
        "email": user_data.get('email', 'unknown'),
    }
    
    response_to_client = JSONResponse(content=user_return_data)

    request.session['user'] = user_data.get('email')
    request.session['access_token'] = access_token

    return response_to_client

@router.get("/api/auth/session")
def check_session(request: Request):
    # 1. Get the token from the cookie sent by the browser
    access_token = request.session.get("access_token")
    
    if not access_token:
        # If no cookie exists, return 401 Unauthorized
        raise HTTPException(status_code=401, detail="No session token found")

    # 2. Use the token to get the user info/validate it against the SSO provider
    # NOTE: Your BE may need to check the token's expiration itself before calling the SSO provider
    user_info_resp = requests.get(
        f"{config.OPENID_PROVIDER_URL}/userinfo",
        headers={'Authorization': f'Bearer {access_token}'}
    )

    if user_info_resp.status_code != 200:
        # If the SSO provider says the token is invalid/expired
        raise HTTPException(status_code=401, detail="Token validation failed or expired")

    # 3. Token is valid. Return the user data to the frontend.
    user_data = user_info_resp.json()
    return {
        "id": user_data.get('sub', user_data.get('email', 'unknown')),
        "email": user_data.get('email', 'unknown'),
    }

@router.post("/api/logout")
def logout(request: Request):
    """
    Clears the server-side session.
    """
    request.session.pop("user", None)
    request.session.pop("access_token", None)
    return JSONResponse(content={"message": "Logged out successfully"})

@router.get("/api/logout")
def logout_get(request: Request):
    """Fallback GET logout endpoint (original behavior)"""
    request.session.pop("user", None)
    request.session.pop("access_token", None)
    return RedirectResponse(config.REDIRECT_URI)
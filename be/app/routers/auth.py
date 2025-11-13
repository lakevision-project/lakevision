import requests
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from starlette.config import Config as AuthlibConfig

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
    user_info_resp = requests.get(f"{config.OPENID_PROVIDER_URL}/userinfo",
                                  headers={'Authorization': f'Bearer {token_data["access_token"]}'})
    user_email = user_info_resp.json()['email']
    request.session['user'] = user_email
    return user_email

@router.get("/api/logout")
def logout(request: Request):
    request.session.pop("user", None)
    return RedirectResponse(config.REDIRECT_URI)
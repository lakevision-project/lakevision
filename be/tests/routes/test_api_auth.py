import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.api import app
from app import config

# Note: The 'client' fixture is provided by conftest.py

def test_root_auth_disabled(client: TestClient, monkeypatch):
    """Test the root endpoint when authentication is turned off."""
    monkeypatch.setattr(config, "AUTH_ENABLED", False)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello, authentication is not enabled."}

def test_root_auth_enabled_not_logged_in(client: TestClient, monkeypatch):
    """Test the root endpoint when auth is on but the user is not logged in."""
    monkeypatch.setattr(config, "AUTH_ENABLED", True)
    # Patch check_auth where it is used in the auth API file
    with patch('app.api.auth.check_auth', return_value=None):
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "You are not logged in."}

def test_root_auth_enabled_logged_in(client: TestClient, monkeypatch):
    """Test the root endpoint when auth is on and the user is logged in."""
    monkeypatch.setattr(config, "AUTH_ENABLED", True)
    # Patch check_auth to simulate a logged-in user
    with patch('app.api.auth.check_auth', return_value="test@example.com"):
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "You are logged in as test@example.com"}

@patch('app.api.auth.oauth.openid.authorize_redirect')
def test_login(mock_authorize_redirect, client: TestClient):
    """Test that the login endpoint calls the OAuth provider."""
    # The mock must be awaitable for async endpoints
    mock_authorize_redirect.return_value = MagicMock()
    
    client.get("/api/login")
    mock_authorize_redirect.assert_called_once()

@patch('requests.post')
@patch('requests.get')
def test_get_token_success(mock_requests_get, mock_requests_post, client: TestClient):
    """Test successful token exchange and user info fetch."""
    mock_requests_post.return_value.status_code = 200
    mock_requests_post.return_value.json.return_value = {"access_token": "fake-access-token"}
    mock_requests_get.return_value.json.return_value = {"email": "user@example.com"}

    response = client.post("/api/auth/token", json={"code": "test-code"})

    assert response.status_code == 200
    assert response.json() == "user@example.com"
    # Check that the session was set
    assert client.cookies.get("session") is not None

@patch('requests.post')
def test_get_token_failure(mock_requests_post, client: TestClient):
    """Test the token exchange when the provider returns an error."""
    mock_requests_post.return_value.status_code = 400
    response = client.post("/api/auth/token", json={"code": "bad-code"})
    assert response.status_code == 400
    assert response.json()["detail"] == "Failed to exchange code for token"

def test_logout(client: TestClient):
    """Test that the logout endpoint returns a redirect."""
    # We test the direct outcome (a redirect response) rather than inspect session state
    response = client.get("/api/logout", follow_redirects=False)
    # Check for a redirect status code (307 is used by FastAPI for temporary redirects)
    assert response.status_code == 307


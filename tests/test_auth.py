import time

from odm_tools.config import settings


def test_initialization(auth_client):
    """Test that the client initializes correctly."""
    assert auth_client.client_id is not None
    assert auth_client.client_secret is not None
    assert auth_client.username is not None
    assert auth_client.password is not None
    assert auth_client.token_url is not None
    assert auth_client._token is None
    assert auth_client._token_expires_at is None


def test_get_token(auth_client):
    """Test getting a token from KeyCloak."""
    token = auth_client.get_token()

    assert token is not None
    assert "access_token" in token
    assert isinstance(token["access_token"], str)
    assert len(token["access_token"]) > 0
    assert "token_type" in token
    assert token["token_type"] == "Bearer"


def test_get_authorization_header(auth_client):
    """Test getting authorization header."""
    header = auth_client.get_authorization_header()

    assert "Authorization" in header
    assert header["Authorization"].startswith("Bearer ")
    assert len(header["Authorization"]) > len("Bearer ")


def test_token_caching(auth_client):
    """Test that tokens are cached and reused."""
    # Get first token
    token1 = auth_client.get_token()

    # Get second token - should be the same (cached)
    token2 = auth_client.get_token()

    assert token1 == token2
    assert token1["access_token"] == token2["access_token"]


def test_is_authenticated(auth_client):
    """Test authentication status."""
    # Should be authenticated after getting a token
    auth_client.get_token()
    assert auth_client.is_authenticated() is True


def test_multiple_header_calls(auth_client):
    """Test multiple authorization header calls work correctly."""
    header1 = auth_client.get_authorization_header()
    header2 = auth_client.get_authorization_header()

    # Should get the same header (cached token)
    assert header1 == header2


def test_token_has_expected_fields(auth_client):
    """Test that token contains expected fields."""
    token = auth_client.get_token()

    # Check required fields
    assert "access_token" in token
    assert "token_type" in token
    assert "expires_in" in token

    # Check field types
    assert isinstance(token["access_token"], str)
    assert isinstance(token["token_type"], str)
    assert isinstance(token["expires_in"], int)


def test_token_expiration_tracking(auth_client):
    """Test that token expiration is tracked correctly."""
    # Get token
    token = auth_client.get_token()

    # Check that expiration time is set
    assert auth_client._token_expires_at is not None
    assert auth_client._token_expires_at > time.time()

    # Should not be expired immediately
    assert not auth_client._is_token_expired()


def test_settings_integration(auth_client):
    """Test that settings are properly loaded."""
    # Verify settings are loaded correctly
    assert auth_client.client_id == settings.ckan.auth.client_id
    assert auth_client.client_secret == settings.ckan.auth.client_secret
    assert auth_client.username == settings.ckan.auth.username
    assert auth_client.password == settings.ckan.auth.password

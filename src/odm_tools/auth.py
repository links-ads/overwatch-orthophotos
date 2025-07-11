import time
from typing import Any

import requests
import structlog
from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc6749.errors import OAuth2Error

log = structlog.get_logger()


class KeyCloakAuthenticator:
    """
    KeyCloak OAuth2 client using Resource Owner Password Credentials Grant.
    Handles token acquisition and refresh automatically.
    """

    def __init__(self, settings=None):
        if settings is None:
            from odm_tools.config import settings

        cfg = settings.ckan.auth
        self.client_id = cfg.client_id
        self.client_secret = cfg.client_secret
        self.username = cfg.username
        self.password = cfg.password
        self.token_url = cfg.url
        self.grant_type = cfg.grant_type if hasattr(cfg, "grant_type") else "password"
        self.scope = cfg.scope if hasattr(cfg, "scope") else "openid"
        # Initialize OAuth2 session
        self.oauth = OAuth2Session(client_id=self.client_id, client_secret=self.client_secret, scope=self.scope)
        self._token = None
        self._token_expires_at = None

    def _is_token_expired(self) -> bool:
        """Check if the current token is expired or will expire soon (within 30 seconds)."""
        if not self._token or not self._token_expires_at:
            return True

        # Add 30 seconds buffer to avoid using tokens that are about to expire
        return time.time() >= (self._token_expires_at - 30)

    def _fetch_new_token(self) -> dict[str, Any]:
        """Fetch a new token using password grant."""
        try:
            token = self.oauth.fetch_token(
                url=self.token_url, username=self.username, password=self.password, grant_type=self.grant_type
            )
            log.debug("Fetched token", token=token)

            # Calculate expiration time
            expires_in = token.get("expires_in", 3600)  # Default to 1 hour
            self._token_expires_at = time.time() + expires_in

            return token

        except OAuth2Error as e:
            raise Exception(f"Failed to obtain OAuth2 token: {e}")
        except Exception as e:
            raise Exception(f"Error fetching token: {e}")

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it has a refresh token and is expired."""
        if not self._token or not self._is_token_expired():
            return

        refresh_token = self._token.get("refresh_token")
        if not refresh_token:
            log.debug("No refresh token available, fetch new token")
            self._token = self._fetch_new_token()
            return

        try:
            # Try to refresh the token
            token = self.oauth.refresh_token(url=self.token_url, refresh_token=refresh_token)

            # Calculate expiration time
            expires_in = token.get("expires_in", 3600)
            self._token_expires_at = time.time() + expires_in
            self._token = token

        except OAuth2Error:
            # Refresh failed, fetch new token
            self._token = self._fetch_new_token()
        except Exception as e:
            raise Exception(f"Error refreshing token: {e}")

    def get_token(self) -> dict[str, Any]:
        """
        Get a valid access token. Automatically handles token refresh if needed.

        Returns:
            Dict containing the token information
        """
        if self._is_token_expired():
            if self._token and self._token.get("refresh_token"):
                self._refresh_token_if_needed()
            else:
                self._token = self._fetch_new_token()
        return self._token  # type: ignore

    def get_authorization_header(self) -> dict[str, str]:
        """
        Get the authorization header for API requests.

        Returns:
            Dict with Authorization header
        """
        token = self.get_token()
        return {"Authorization": token["access_token"]}

    def revoke_token(self) -> None:
        """
        Revoke the current token (logout).
        Note: This requires the revocation endpoint to be available.
        """
        if not self._token:
            return
        # Try to revoke the token
        revoke_url = self.token_url.replace("/token", "/revoke")
        try:
            requests.post(
                revoke_url,
                data={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "token": self._token.get("access_token"),
                    "token_type_hint": "access_token",
                },
            )
        except Exception:
            # Revocation failed, but we'll clear the token anyway
            pass
        finally:
            self._token = None
            self._token_expires_at = None

    def is_authenticated(self) -> bool:
        """
        Check if we have a valid token.

        Returns:
            True if authenticated, False otherwise
        """
        try:
            token = self.get_token()
            return token is not None and "access_token" in token
        except Exception:
            return False

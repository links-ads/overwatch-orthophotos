import requests

from odm_tools.config import settings


class KeyCloakAuth:
    def __init__(self):
        cfg = settings.ckan.auth
        self.username = cfg.username
        self.password = cfg.password
        self.client_id = cfg.client_id
        self.grant_type = cfg.grant_type
        self.scope = cfg.scope
        self.client_secret = cfg.client_secret
        self.api_key = cfg.api_key
        self.login_url = cfg.url
        self.access_token: str | None = None
        self.refresh_token: str | None = None

    def authenticate(self):
        """
        Authenticates with the Keycloak server and retrieves an access token and refresh token.

        Args:
            url (str): The URL of the Keycloak server.

        Returns:
            tuple: A tuple containing the access token and refresh token.

        Raises:
            HTTPError: If the authentication request fails or returns an unsuccessful status code.
        """
        data = {
            "username": self.username,
            "password": self.password,
            "client_id": self.client_id,
            "grant_type": self.grant_type,
            "scope": self.scope,
            "client_secret": self.client_secret,
        }
        header = {"Content-Type": "application/x-www-form-urlencoded"}
        response = requests.post(self.login_url, data=data, headers=header)
        response.raise_for_status()
        self.access_token = response.json()["access_token"]
        self.refresh_token = response.json()["refresh_token"]
        return self.access_token, self.refresh_token

    def authorization_header(self):
        """
        Returns a dictionary with the access token for the Authorization header.

        Returns:
            dict: A dictionary with the Authorization header.
        """
        if not self.access_token:
            raise Exception("Access token not set")

        return {"Authorization": f"{self.access_token}"}


from oauthlib.oauth2 import WebApplicationClient
from requests_oauthlib import OAuth2Session


class KeyCloakAuthOAuth:
    def __init__(self):
        cfg = settings.ckan.auth
        self.client_id = cfg.client_id
        self.client_secret = cfg.client_secret
        self.username = cfg.username
        self.password = cfg.password
        self.token_url = cfg.url
        self.oauth = OAuth2Session(client=WebApplicationClient(client_id=self.client_id))
        self.token = None

    def get_token(self):
        if not self.token:
            self.token = self.oauth.fetch_token(
                token_url=self.token_url,
                username=self.username,
                password=self.password,
                client_secret=self.client_secret,
            )
        return self.token

    def get_authorization_header(self):
        token = self.get_token()
        return {"Authorization": f"Bearer {token['access_token']}"}

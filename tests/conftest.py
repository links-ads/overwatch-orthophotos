from pathlib import Path
from unittest.mock import Mock

import pytest

from odm_tools.auth import KeyCloakAuthenticator
from odm_tools.uploader import CKANUploader


@pytest.fixture
def rmq_settings():
    """Create mock RabbitMQ settings for testing."""
    mock_settings = Mock()
    mock_settings.rmq = Mock()
    mock_settings.rmq.host = "test-host"
    mock_settings.rmq.port = 5672
    mock_settings.rmq.vhost = "/test"
    mock_settings.rmq.username = "test-user"
    mock_settings.rmq.password = "test-pass"
    mock_settings.rmq.ssl = True
    return mock_settings


@pytest.fixture
def auth_client():
    """Create KeyCloakAuthOAuth instance with real settings."""
    return KeyCloakAuthenticator()


@pytest.fixture
def ckan_uploader():
    """Create CKANUploader instance for testing."""
    return CKANUploader()


@pytest.fixture
def test_file():
    """Create a temporary test file for upload."""
    test_dir = Path(__file__).parent
    test_file_path = test_dir / "data" / "test_orthophoto.tif"
    if not test_file_path.exists():
        raise FileNotFoundError(f"Test file not found: {test_file_path}")
    return test_file_path

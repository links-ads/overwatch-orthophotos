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


@pytest.fixture
def request_file():
    """Create a temporary test file for upload."""
    test_dir = Path(__file__).parent
    request_file_path = test_dir / "data" / "test_request.json"
    if not request_file_path.exists():
        raise FileNotFoundError(f"Test file not found: {request_file_path}")
    return request_file_path


@pytest.fixture
def valid_request_data():
    """Sample valid request data matching the expected structure."""
    return {
        "requestId": "test-request-123",
        "situationId": "test-situation-456",
        "start": "2024-10-01T10:00:00Z",
        "end": "2024-10-01T12:00:00Z",
        "datatypeIds": [22002, 22003],
        "feature": {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-8.472698427917976, 40.558658193522064],
                        [-8.472698427917976, 40.405542218603046],
                        [-7.472698427917976, 40.405542218603046],
                        [-7.472698427917976, 40.558658193522064],
                        [-8.472698427917976, 40.558658193522064],
                    ]
                ],
            },
        },
    }


@pytest.fixture
def valid_multipolygon_data():
    """Sample request with MultiPolygon geometry."""
    return {
        "requestId": "test-request-multipolygon",
        "situationId": "test-situation-789",
        "start": "2024-10-01T10:00:00Z",
        "end": "2024-10-01T12:00:00Z",
        "datatypeIds": [22002],
        "feature": {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    [
                        [
                            [-8.5, 40.5],
                            [-8.5, 40.4],
                            [-8.4, 40.4],
                            [-8.4, 40.5],
                            [-8.5, 40.5],
                        ]
                    ],
                    [
                        [
                            [-7.5, 40.5],
                            [-7.5, 40.4],
                            [-7.4, 40.4],
                            [-7.4, 40.5],
                            [-7.5, 40.5],
                        ]
                    ],
                ],
            },
        },
    }

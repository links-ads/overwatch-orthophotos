from unittest.mock import Mock

import pytest


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

from odm_tools.notifier import RabbitMQNotifier  # Replace with actual import path


def test_rabbitmq_connection():
    """Test that RabbitMQ connection works with real config."""
    # This will read from config.yml automatically
    notifier = RabbitMQNotifier()

    # Test that connection and channel are created
    assert notifier.connection is not None
    assert notifier.channel is not None
    assert not notifier.connection.is_closed
    assert not notifier.channel.is_closed


def test_check_connection_works():
    """Test that check_connection method works."""
    notifier = RabbitMQNotifier()

    # Should not raise any exceptions
    notifier.check_connection()

    # Connection should still be open
    assert not notifier.connection.is_closed
    assert not notifier.channel.is_closed


def test_multiple_connections():
    """Test that multiple notifier instances work."""
    notifier1 = RabbitMQNotifier()
    notifier2 = RabbitMQNotifier()

    # Both should have working connections
    assert not notifier1.connection.is_closed
    assert not notifier2.connection.is_closed

    # They should be different connection objects
    assert notifier1.connection != notifier2.connection

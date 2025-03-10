import unittest

from unittest.mock import patch, MagicMock

from src.createEvent.event_producer import EventProducer


class TestEventProducer(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.item = {
            "PK": {"S": "PK"},
            "SK": {"S": "PREFIX-123"},
            "name": {"S": "John"},
            "description": {"S": "Description 1"},
            "createdAt": {"S": "2021-01-01T00:00:00"}
        }
        self.producer = EventProducer()

    async def test_convert_to_event(self):
        expected_event = {
            "PK": "PK",
            "SK": "PREFIX-123",
            "name": "John",
            "description": "Description 1",
            "createdAt": "2021-01-01T00:00:00"
        }

        self.assertEqual(self.producer.convert_to_event(self.item), expected_event)

    @patch('aiobotocore.session.get_session')
    async def test_send_event_success(self, mock_get_session):
        # Setup
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.create_client.return_value.__aenter__.return_value = mock_client

        await self.producer.send_event([self.item])

        mock_client.send_message.assert_called_once()
        _, kwargs = mock_client.send_message.call_args
        self.assertEqual(kwargs['QueueUrl'], self.producer.queue_url)
        self.assertIn('MessageBody', kwargs)

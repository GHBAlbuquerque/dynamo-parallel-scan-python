import unittest

from unittest.mock import patch, MagicMock

from src.createEvent.event_producer import EventProducer

class TestEventProducer(unittest.IsolatedAsyncioTestCase):
    
    def setUp(self):
        self.item = {
                    "PK": {"S": 'e7ba03fa-7067-45a4-92e6-225db357dc86'},
                    "SK": {"S": "PREFIX-444"},
                    "name": {"S": "John"},
                    "description": {"S" : f"Description 0"},
                    "createdAt": {"S" : "2025-02-30T00:00:00"}
                    }
        self.event_producer = EventProducer()


    def test_convert_to_event(self):
        expected_event = {
            "PK": "e7ba03fa-7067-45a4-92e6-225db357dc86",
            "SK": "PREFIX-444",
            "name": "John",
            "description": "Description 0",
            "createdAt": "2025-02-30T00:00:00"
        }

        event = self.event_producer.convert_to_event(self.item)
        self.assertEqual(event, expected_event)

    @patch('aiobotocore.session.get_session') # Decorater from unittest.mock to mock the get_session function
    async def test_send_event(self, mock_get_session):
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_get_session.return_value = mock_session

         # Configure the mock session to return the mock client
        mock_session.create_client.return_value.__aenter__.return_value = mock_client # configure a mock object for an asynchronous context manager

        # Call the function being tested    
        await self.event_producer.send_event([self.item])

        # Assert that get_session was called
        mock_client.send_message.assert_called_once()
        
        # Validate the arguments passed to the send_message
        args, kwargs = mock_client.send_message.call_args
        self.assertEqual(kwargs['QueueUrl'], 'http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/parallel-scan-queue')
        self.assertIn('MessageBody', kwargs)
import unittest

from unittest.mock import patch, MagicMock

from src.createEvent.event_producer import EventProducer
from src.tableScan.scan_by_segment import TableScanProcessor


class TestEventProducer(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.mock_event_producer = MagicMock(spec=EventProducer)
        self.table_scan_processor = TableScanProcessor(self.mock_event_producer)

    @patch('aiobotocore.session.get_session')
    async def test_scan_dynamodb_with_segments_success(self, mock_get_session):
        # Setup
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.create_client.return_value = mock_client
        mock_client.scan.side_effect = [
            {'Items': [{'id': {'S': '1'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key1'},
            {'Items': [{'id': {'S': '2'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key2'},
            {'Items': [{'id': {'S': '3'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key3'},
            {'Items': [{'id': {'S': '4'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key4'},
            {'Items': [{'id': {'S': '5'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key5'},
            {'Items': [{'id': {'S': '6'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key6'},
            {'Items': [{'id': {'S': '7'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key7'},
            {'Items': [{'id': {'S': '8'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key8'},
            {'Items': [{'id': {'S': '9'}, 'name': {'S': 'John Doe'}}], 'LastEvaluatedKey': 'key9'},
            {'Items': [{'id': {'S': '10'}, 'name': {'S': 'John Doe'}}]}
        ]

        table_name = 'test_table'
        attribute_name = 'test_attribute'
        sk_filter = 'test_filter'
        segment = 2
        total_segments = 2
        limit_of_rows = 10

        # Act
        result = await self.table_scan_processor.scan_dynamodb_with_segments(
            table_name,
            attribute_name,
            sk_filter,
            segment,
            total_segments,
            limit_of_rows
        )

        # Assert
        self.assertEqual(result, {
            "SEGMENT": segment,
            "TOTAL_SEGMENTS": total_segments,
            "STATUS": "SUCCESS"
        })
        self.assertEqual(mock_client.scan.call_count, 10)

    @patch('aiobotocore.session.get_session')
    async def test_scan_dynamodb_with_segments_error(self, mock_get_session):
        # Setup
        mock_session = MagicMock()
        mock_client = MagicMock()
        mock_get_session.return_value = mock_session
        mock_session.create_client.return_value = mock_client
        mock_client.scan.side_effect = Exception('Scan Error')

        table_name = 'test_table'
        attribute_name = 'test_attribute'
        sk_filter = 'test_filter'
        segment = 0
        total_segments = 2
        limit_of_rows = 10

        # Act & Assert
        with self.assertRaises(Exception) as context: (

            await self.table_scan_processor.scan_dynamodb_with_segments(
                table_name,
                attribute_name,
                sk_filter,
                segment,
                total_segments,
                limit_of_rows
            ))

        self.assertEqual(str(context.exception), 'Scan Error')

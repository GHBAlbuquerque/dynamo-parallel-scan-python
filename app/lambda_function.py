import asyncio

from src.createEvent.event_producer import EventProducer
from src.tableScan.scan_by_segment import TableScanProcessor

def lambda_handler(event: dict, context): 

    message_producer = EventProducer()
    table_scan_processor = TableScanProcessor(message_producer)
    loop = asyncio.get_event_loop()

    result = loop.run_until_complete(
            table_scan_processor.scan_dynamodb_with_segments(
                    'TestTable', #table-name
                    'PK', #my-pk-filter
                    'SK', #my-sk-filter
                    event["current_segment"],
                    event["total_segments"],
                    event["limit_of_rows"]
            )
    )

    return _build_output(result)

def _build_output(result):
    return {
        "result": result
    }
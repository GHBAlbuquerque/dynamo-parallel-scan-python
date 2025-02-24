import asyncio
import yaml

from src.createEvent.event_producer import EventProducer
from src.tableScan.scan_by_segment import TableScanProcessor

with open('app/config.yaml', 'r') as file:
    config = yaml.safe_load(file)

def local_handler(current_segment): 

    print("Starting table scan...")

    message_producer = EventProducer()
    table_scan_processor = TableScanProcessor(message_producer)
    loop = asyncio.get_event_loop()

    result = loop.run_until_complete(
            table_scan_processor.scan_dynamodb_with_segments(
                    config['dynamodb']['table_name'], #table-name
                    config['scan']['attribute_name'], #my-sk-filter
                    config['scan']['sk_filter'], #my-sk-value-prefix
                    current_segment,
                    config['scan']['total_segments'], #my-sk-value-prefix,
                    config['scan']['limit_of_rows'], #my-sk-value-prefix
            )
    )

    print("Ending table scan...")

    return _build_output(result)


def _build_output(result):
    return {
        "result": result
    }
    

local_handler(0)
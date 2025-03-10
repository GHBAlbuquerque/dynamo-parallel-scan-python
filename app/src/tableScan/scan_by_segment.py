import asyncio
import yaml

from typing import Any
from aiobotocore.session import get_session
from src.createMessage.MessageProducer import MessageProducer
from src.createEvent.event_producer import EventProducer
from aws_lambda_powertools import Logger

logger = Logger()


class TableScanProcessor:

    def __init__(self, event_producer : EventProducer):
        self.event_producer = event_producer


    async def scan_dynamodb_with_segments(self, table_name, attribute_name,
                                          sk_filter, segment, total_segments,
                                          limit_of_rows, batch_size=500) -> dict[str, Any]:
        
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)

            session = get_session()
            async with session.create_client('dynamodb', 
                                            region_name=config['dynamodb']['region'],
                                            endpoint_url=config['dynamodb']['endpoint_url']
                                            ) as client: # Create Dynamo client for connection
                try:
                    filter_expression = f"begins_with({attribute_name}, :sk_filter)"
                    expression_attribute_values = {":sk_filter": {"S": sk_filter}}

                    # First scan
                    response = await client.scan( 
                        TableName=table_name,
                        FilterExpression=filter_expression,
                        ExpressionAttributeValues=expression_attribute_values,
                        Segment=segment,
                        TotalSegments=total_segments,
                        Limit=limit_of_rows
                    ) 

                    logger.info(f"Number of rows fecthed on first scan: {len(response.get('Items', []))}") # Get returns the items in the response or an empty array

                    #Count first scan before getting into loop
                    num_of_scans=1 
                    items_to_send=response.get('Items', [])
                    tasks = []

                    # Get the last evaluated key from the response or nothing   
                    exclusive_start_key = response.get('LastEvaluatedKey', None) 

                    # Subsequent scans
                    while 'LastEvaluatedKey' in response:
                        response = await client.scan( 
                            TableName=table_name,
                            FilterExpression=filter_expression,
                            ExpressionAttributeValues=expression_attribute_values,
                            Segment=segment,
                            TotalSegments=total_segments,
                            Limit=limit_of_rows,
                            ExclusiveStartKey=response['LastEvaluatedKey']
                        )
                        exclusive_start_key = response.get('LastEvaluatedKey', None)
                        num_of_scans+=1

                        # Process in batches immediately
                        for item in response.get('Items', []):
                            items_to_send.append(item)
                            if len(items_to_send) >= batch_size:
                                task = asyncio.create_task(self.event_producer.send_event(items_to_send))
                                tasks.append(task)
                                items_to_send = []

                    # Send reamining items
                    if items_to_send:
                        task = asyncio.create_task(self.event_producer.send_event(items_to_send))
                        tasks.append(task)

                    await asyncio.gather(*tasks)

                    logger.info(f"Number of scans: {num_of_scans}")

                except Exception as e:
                    logger.error(f"Error scanning dynamoDB table: {e}")
                    raise e

        return {
            "SEGMENT": segment,
            "TOTAL_SEGMENTS": total_segments,
            "STATUS": "SUCCESS"
        }
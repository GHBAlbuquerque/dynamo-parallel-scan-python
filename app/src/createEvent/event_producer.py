import asyncio
import json
import yaml

from aiobotocore.session import get_session
from aws_lambda_powertools import Logger

from src.createMessage.MessageProducer import MessageProducer

logger = Logger()

class EventProducer(MessageProducer):

    def __init__(self):
        with open('app/config.yaml', 'r') as file:
            config = yaml.safe_load(file)

            self.session = get_session()
            self.queue_url = config['sqs']['queue_url'] # Your created queue URL
            self.endpoint_url = config['sqs']['endpoint_url']

    # Event sender method
    async def send_event(self, items):
        logger.info(f"Sending {len(items)} items to the queue")
        semaphore = asyncio.Semaphore(5) # Limit the number of concurrent requests
        
        async with self.session.create_client('sqs', region_name='us-east-1', endpoint_url=self.endpoint_url) as client:
            tasks = [self.send_message_with_semaphore(semaphore, client, item) for item in items] # Is a list comprehension in Python that creates a list of coroutine objects. Each coroutine object is an instance of the send_message_with_semaphore coroutine, which is called for each item in the items list.
            items.clear()
            await asyncio.gather(*tasks)
        logger.info("All items sent to the queue")

    # Send message with semaphore
    async def send_message_with_semaphore(self, semaphore, client, event):
        async with semaphore:
            await self.send_message(client, event)

    # Coroutines: The send_message_with_semaphore method is an asynchronous coroutine defined with async def. 
    # When called, it returns a coroutine object that can be awaited.


    async def send_message(self, client, event):
        try:
            await client.send_message(
                QueueUrl=self.queue_url,
                MessageBody=json.dumps(event)
            )
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    
    def convert_to_event(self, item):
        return {
            "PK": item.get("PK", {}).get("S", ""),
            "SK": item.get("SK", {}).get("S", ""),
            "name": item.get("name", {}).get("S", ""),
            "description": item.get("description", {}).get("S", ""),
            "createdAt": item.get("createdAt", {}).get("S", "")
        }



import json
import uuid
import random
from datetime import datetime, timedelta

def random_date(start, end):
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds()))
    )

def generate_dynamodb_records_json(file_name, num_records): 
    with open(file_name, mode='w') as file: 
        for i in range(num_records): 

            # Generate random PK
            pk = str(uuid.uuid4())

            # Generate random SK
            sk = random.randint(100, 999)

            record = {
                "PK": {"S": pk},
                "SK": {"N": str(sk)},
                "name": {"S": random.choice(["John", "Jane", "Alice", "Bob", "Charlie"])},
                "description": f"Description {i}",
                "created_at": random_date(datetime(2020, 1, 1), datetime.now()).isoformat()
            }
            
            file.write(json.dumps(record) + '\n')

generate_dynamodb_records_json('dynamodb_records.json', 1000)
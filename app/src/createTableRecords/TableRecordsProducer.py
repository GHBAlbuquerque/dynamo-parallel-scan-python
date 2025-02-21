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
            record = {
                "id": str(uuid.uuid4()),
                "name": f"Name {i}",
                "age": random.randint(18, 100),
                "created_at": random_date(datetime(2020, 1, 1), datetime.now()).isoformat()
            }
            file.write(json.dumps(record) + '\n')
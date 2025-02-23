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
        
        file.write("{\"TestTable\": [\n")

        for i in range(num_records): 

            # Generate random PK
            pk = str(uuid.uuid4())

            # Generate random SK
            sk = random.randint(100, 999)

            record = {
                "PutRequest": {
                    "Item": {
                        "PK": {"S": pk},
                        "SK": {"S": str(sk)},
                        "name": {"S": random.choice(["John", "Jane", "Alice", "Bob", "Charlie"])},
                        "description": {"S" : f"Description {i}"},
                        "createdAt": {"S" :random_date(datetime(2020, 1, 1), datetime.now()).isoformat()}
                    }
                }
            }
            
            if(i == num_records - 1):
                file.write(json.dumps(record) + '\n')
                break
            
            file.write(json.dumps(record) + ',\n')

        file.write("]}")

generate_dynamodb_records_json('dynamodb_records_batch_write.json', 200)
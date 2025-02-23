import boto3
import json

from createTableRecords.TableRecordsProducerJSONImportFile import generate_dynamodb_records_json

dynamodbclient=boto3.resource('dynamodb', endpoint_url='http://localhost:4566')

table = dynamodbclient.Table('TestTable')

with open('dynamodb_records.json', 'r') as myfile:
    data=myfile.read()

generate_dynamodb_records_json('dynamodb_records.json', 5000)

try: 
    objects = json.loads(data)

    print(len(objects))

    for object in objects:
        #print(object)
        table.put_item(Item=object)

    print("Finished importing") 

except Exception as e:
    print(f"Failed to import records: {e}")

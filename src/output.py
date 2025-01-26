import boto3

##NOT TESTED AS I DON"T HAVE DYANAMO DB ACCESS

dynamodb = boto3.resource('dynamodb')
table_name = 'IoTLocationUpdates'
table = dynamodb.Table(table_name)

def store_in_dynamodb(data):
    """Update container data in DynamoDB."""
    table.update_item(
        Key={
            'container_id': data['container_id']
        },
        UpdateExpression="SET #loc = :loc, #ts = :ts, #st = :st, #dest = :dest, #proc_ts = :proc_ts",
        ExpressionAttributeNames={
            '#loc': 'location',
            '#ts': 'timestamp',
            '#st': 'status',
            '#dest': 'destination_depot',
            '#proc_ts': 'processing_timestamp'
        },
        ExpressionAttributeValues={
            ':loc': data['location'],
            ':ts': data['timestamp'],
            ':st': data['status'],
            ':dest': data['destination_depot'],
            ':proc_ts': data['processing_timestamp']

        }
    )

def store_in_dynamodb_fake(data):
    print(f"Storing data in DynamoDB: {data}")
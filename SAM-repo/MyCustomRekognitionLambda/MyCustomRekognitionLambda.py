import json
import boto3
import os
import io
import datetime
from urllib.parse import unquote_plus


def lambda_handler(event, context):
    model_arn=os.environ["CustomLabels_ModelArn"]
    MinimumConfidence = int(os.environ["Confidence_Threshold"])
    DynamoTable = os.environ["DynamoDB_Table"]
    
    client_dynamodb=boto3.client('dynamodb')
    client_rekognition=boto3.client('rekognition')

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        location = "s3://" + bucket + "/" + key
                
        #process using S3 object
        response_custom = client_rekognition.detect_custom_labels(Image={'S3Object': {'Bucket': bucket, 'Name': key}},
            MinConfidence=MinimumConfidence,ProjectVersionArn=model_arn)
        
        
        #Get the custom labels
        labels_custom=response_custom['CustomLabels']


        response = client_dynamodb.put_item(TableName=DynamoTable,
           Item={
                'ID': {'S':key},
                'TimeStamp': {'S': str(datetime.datetime.now())},
                'Inference': {'S': json.dumps(labels_custom)},
                'Location': {'S': location}
                }
        )
                   

    
    return {        
    	'statusCode': 200,        
    	'body': 'CustomLabels: '+ json.dumps(labels_custom)
}
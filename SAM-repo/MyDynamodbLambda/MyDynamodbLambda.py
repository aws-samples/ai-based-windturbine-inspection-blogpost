import json
import os
import boto3
from urllib.parse import urlparse

env1 = os.environ['AlarmingLabelList']
AlarmingLabelList = env1.split(",")
SNSTopic = os.environ['TopicArn']
Threshold = float(os.environ['Threshold'])/100.


def create_presigned_url(location, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """
    lnk = urlparse(location, allow_fragments=True)
    bucket_name = lnk.netloc
    object_name = lnk.path.lstrip('/')

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3')
    response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)

    # The response contains the presigned URL
    return response

def lambda_handler(event, context):

    for record in event['Records']:
        InferenceOutput = json.loads(record['dynamodb']['NewImage']['Inference']['S'])
        loc = record['dynamodb']['NewImage']['Location']['S']
        link = create_presigned_url(loc)
        inf = 0
        message = ''
        for inference in InferenceOutput:
            inf += 1
            if inference['Name'] in AlarmingLabelList and inference['Confidence'] > Threshold:
                alarm = 'ALARM! :' + inference['Name'] + ' (Confidence: {})'.format(inference['Confidence'])
                message = message + '\n' + alarm
        if message:
            message = message + '\n' + 'Please see the photo: ' + link
    
    client = boto3.client('sns')
    response = client.publish(
        TopicArn=SNSTopic,
        Message= message,
        Subject= 'A Possible issue detected at Turbine',
        MessageStructure='string'
        )
        
    return response
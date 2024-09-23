''' This set of functions are used to CRUD a SNS topic and SQS queue '''

import os
import json
import boto3
from .wrappers.sns_wrapper import SnsWrapper
from .wrappers.sqs_wrapper import SqsWrapper

SNS_PATH = os.path.abspath("config/aws/sns/sns_credentials.json")
SQS_PATH = os.path.abspath("config/aws/sqs/sqs_credentials.json")

# SNS

def sns_connection():
    ''' Create a client connection to SNS'''

    with open(SNS_PATH, 'r', encoding="utf-8") as f:
        sns_config = json.load(f)

    sns = boto3.client(
            service_name=sns_config['sns']["service_name"],
            region_name=sns_config['sns']["region_name"],
            aws_access_key_id =sns_config['sns']["aws_access_key_id"],
            aws_secret_access_key = sns_config['sns']["aws_secret_access_key"],
    )
    return sns

def sns_send_message(sns_client, topic, message, subject):
    ''' SNS publishes a message on a topic that sends to 'email' and 'sqs' queue '''
    message_id = SnsWrapper.publish_multi_message(boto_client=sns_client,
                                     topic=topic,
                                     subject=subject,
                                     message=message)
    return {"message":message_id}

def sns_create_topic(sns_client, topic_name):
    ''' Create a new SNS topic '''
    sns = SnsWrapper(sns_client)
    return sns.create_topic(topic_name)

def sns_list_topics(sns_client):
    ''' List all SNS topic '''
    sns = SnsWrapper(sns_client)
    return sns.list_topics()

def sns_delete_topic(sns_client, topic_arn):
    ''' Deletes specific topic from SNS '''
    return SnsWrapper.delete_topic(sns_client,topic_arn)

def sns_subscribe(sns_client, topic_arn, protocol, endpoint):
    ''' Subscribe a service to an existing SNS topic '''
    return SnsWrapper.subscribe(sns_client, topic_arn, protocol, endpoint)

# SQS

def sqs_connection():
    ''' Create a client connection to SQS'''

    with open(SQS_PATH, 'r', encoding="utf-8") as f:
        sqs_config = json.load(f)

    sqs = boto3.client(
            service_name=sqs_config['sqs']["service_name"],
            region_name=sqs_config['sqs']["region_name"],
            aws_access_key_id =sqs_config['sqs']["aws_access_key_id"],
            aws_secret_access_key = sqs_config['sqs']["aws_secret_access_key"],
    )
    return sqs

def sqs_receive_message(sqs_client, queue_url, max_messages):
    ''' Receives a message from a SQS queue and remove it from the queue 
        so other consumers dont get the same message
    '''
    response = SqsWrapper.receive_message(sqs_client, queue_url, max_messages)

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']

    sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    return {"message":message}

def sqs_create_queue(sqs_client, queue_name):
    ''' Create a new SQS queue'''
    sqs = SqsWrapper(sqs_client)
    return sqs.create_queue(queue_name)

def sqs_list_queues(sqs_client, prefix=None):
    ''' Listing SQS queues'''
    sqs = SqsWrapper(sqs_client)
    return sqs.list_queues(prefix)

def sqs_send_message(sqs_client,queue,message_body,message_attributes=None):
    ''' Send message to SQS queue'''
    return SqsWrapper.send_message(sqs_client, queue, message_body, message_attributes)

def sqs_delete_queue(sqs_client,queue):
    ''' Delete SQS queue'''
    return SqsWrapper.remove_queue(sqs_client,queue)

''' This file is responsable for creating all endpoints'''

from flask import request
from src.messages_handler import sns_connection,\
                                sqs_connection,\
                                sns_send_message,\
                                sqs_receive_message,\
                                sns_create_topic,\
                                sns_list_topics,\
                                sns_delete_topic,\
                                sns_subscribe,\
                                sqs_create_queue,\
                                sqs_list_queues,\
                                sqs_send_message,\
                                sqs_delete_queue
from app import app

@app.route("/send", methods=['POST'])
def sns_sending_message():
    ''' This http request sends a message to an existing sqs queue and email
        :param: topic: ARN of the topic
        :param: messages: json with format {"default":"message","email":"message","sqs":"message"}
        :param: subject: string of the subject for the 'email' message
    '''
    data = request.get_json()
    topic = data.get("topic")
    message = data.get("messages")
    subject = data.get("subject")
    sns = sns_connection()
    return sns_send_message(sns, topic, message, subject)

@app.route("/create_topic", methods=['POST'])
def sns_creating_topic():
    ''' Creates a new SNS topic
        :param: json with the topic name using key "topic_name"
    '''
    data = request.get_json()
    topic_name = data.get("topic_name")
    sns = sns_connection()
    return sns_create_topic(sns, topic_name)

@app.route("/list_topics", methods=['GET'])
def sns_listing_topic():
    ''' Lists SNS topics '''
    sns = sns_connection()
    return sns_list_topics(sns)

@app.route("/delete_topic", methods=['POST'])
def sns_deleting_topic():
    ''' Deletes a topic from SNS
        :param: json with the topic arn using key "topic_arn"
    '''
    data = request.get_json()
    topic_arn = data.get("topic_arn")

    sns = sns_connection()
    return sns_delete_topic(sns, topic_arn)

@app.route("/subscribe_topic", methods=['POST'])
def sns_subscribing_topic():
    ''' Creates a subscription to SNS topic
        :param: topic ARN with key "topic_arn"
        :param: protocol with key "protocol" (such as 'sms' or 'email')
        :param: endpoint with key "endpoint" ('phone' or 'email address')
    '''
    data = request.get_json()
    topic_arn = data.get("topic_arn")
    protocol = data.get("protocol")
    endpoint = data.get("endpoint")

    sns = sns_connection()
    return sns_subscribe(sns_client=sns,endpoint=endpoint,protocol=protocol,topic_arn=topic_arn )

@app.route("/get_messages", methods=['POST'])
def reading_from_queue():
    '''
    This request reads messages from SQS queue
    :param: url: URL of the queue to read from 
    '''
    sqs = sqs_connection()
    data = request.get_json()
    queue_url = data.get("url")
    return sqs_receive_message(sqs, queue_url, 1)

@app.route("/create_queue", methods=['POST'])
def creating_from_queue():
    ''' Create a new SQS queue
        :param: queue name with json key "queue_name"
    '''
    sqs_client = sqs_connection()
    data = request.get_json()
    queue_name = data.get("queue_name")
    return sqs_create_queue(sqs_client, queue_name)

@app.route("/list_queues", methods=['GET'])
def listing_queues():  
    ''' List SQS queues'''
    sqs_client = sqs_connection()
    return sqs_list_queues(sqs_client)


@app.route("/sqs_send_message", methods=['POST'])
def sending_for_queue():
    ''' Sending message to a SQS queue
        :param: queue url with json key "queue_url"
        :param: message with key "message"
    '''
    sqs_client = sqs_connection()
    data = request.get_json()
    message_body = data.get("message")
    queue_url = data.get("queue_url")
    return sqs_send_message(sqs_client, queue_url, message_body)

@app.route("/delete_queue", methods=['POST'])
def sqs_deleting_queue():
    ''' Deletes a queue from SQS
        :param: json with the queue url using key "queue_url"
    '''
    data = request.get_json()
    queue_url = data.get("queue_url")
    sqs = sqs_connection()
    return sqs_delete_queue(sqs, queue_url)

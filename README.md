This is a very simple Flask API to access AWS SNS and SQS services.

# SNS:
### /send : send messages to a existing topic
### /create_topic: create a topic
### /list_topics: list all topics
### /delete_topic: delete a topic
### /subscribe_topic: subscribe a service to a topic

# SQS:
### /get_messages: read messages from queue
### /create_queue: create a queue
### /list_queues: list all queues
### /sqs_send_message: send message to a queue (sqs client)
### /delete_queue: delete queue




Documentation: 
##### https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
##### https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html

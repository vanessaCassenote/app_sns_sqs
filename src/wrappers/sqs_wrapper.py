''' This class should be used to interact with SQS and its queues '''

import logging
from botocore.exceptions import ClientError

logging.basicConfig(filename="log.log",format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SqsWrapper:
    ''' Encapsulates Amazon SQS queues'''

    def __init__(self, sqs):
        self.sqs = sqs

    def create_queue(self, name, attributes=None):
        """
        Creates an Amazon SQS queue.

        :param name: The name of the queue. This is part of the URL assigned to the queue.
        :param attributes: The attributes of the queue, such as maximum message size or
                        whether it's a FIFO queue.
        :return: A Queue object that contains metadata about the queue and that can be used
                to perform queue operations like sending and receiving messages.
        """
        if not attributes:
            attributes = {}

        try:
            queue = self.sqs.create_queue(QueueName=name, Attributes=attributes)
            logger.info("Created queue '%s' with URL=%s", name, queue["QueueUrl"])
        except ClientError as error:
            logger.exception("Couldn't create queue named '%s'.", name)
            raise error
        else:
            return queue

    def list_queues(self, prefix=None):
        """
        Gets a list of SQS queues. When a prefix is specified, only queues with names
        that start with the prefix are returned.

        :param prefix: The prefix used to restrict the list of returned queues.
        :return: A list of Queue objects.
        """

        if prefix:
            queue_iter = self.sqs.queues.list_queues(QueueNamePrefix=prefix)
        else:
            queue_iter = self.sqs.list_queues()

        queues = queue_iter["QueueUrls"]
        if queues:
            logger.info("Got queues: %s", ", ".join([q for q in queues]))
        else:
            logger.warning("No queues found.")
        return queue_iter

    @staticmethod
    def send_message(sqs_client, queue, message_body, message_attributes=None):
        """
        Send a message to an Amazon SQS queue.

        :param queue: The queue that receives the message.
        :param message_body: The body text of the message.
        :param message_attributes: Custom attributes of the message. These are key-value
                                pairs that can be whatever you want.
        :return: The response from SQS that contains the assigned message ID.
        """
        if not message_attributes:
            message_attributes = {}

        try:
            response = sqs_client.send_message(
            QueueUrl=queue,MessageBody=message_body, MessageAttributes=message_attributes
            )
        except ClientError as error:
            logger.exception("Send message failed: %s", message_body)
            raise error
        else:
            return response

    @staticmethod
    def receive_message(queue, QueueUrl, MaxNumberOfMessages):
        """
        Receive a batch of messages in a single request from an SQS queue.

        :param queue: The queue from which to receive messages.
        :param SentTimestamp: Time the message was sent to the queue (milliseconds).
        :param max_number: The maximum number of messages to receive. The actual number
                        of messages received might be less.
        :param MessageAttributeNames: list of attribute names to receive (it is possible to use a prefix)
        :return: The the Message objects received. It contains the body
                of the message and metadata and custom attributes.
        """
        try:
            messages = queue.receive_message(
                    QueueUrl=QueueUrl,
                    AttributeNames=[
                        'SentTimestamp'
                    ],
                    MaxNumberOfMessages=MaxNumberOfMessages,
                    MessageAttributeNames=[
                        'All'
                    ],
                    VisibilityTimeout=0,
                    WaitTimeSeconds=0
                )
        except ClientError as error:
            logger.exception("Couldn't receive messages from queue: %s", queue)
            raise error
        else:
            return messages

    @staticmethod
    def remove_queue(client, queue):
        """
        Removes an SQS queue. When run against an AWS account, it can take up to
        60 seconds before the queue is actually deleted.

        :param queue: The queue to delete.
        :return: None
        """
        try:
            response = client.delete_queue(
                QueueUrl=queue
            )
            logger.info("Deleted queue with URL=%s.", response)
        except ClientError as error:
            logger.exception("Couldn't delete queue with URL=%s!", response)
            raise error
        else:
            return queue

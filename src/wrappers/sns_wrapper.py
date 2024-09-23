''' This class should be used to interact with SNS and its topics '''

import json
import logging
from botocore.exceptions import ClientError

logging.basicConfig(filename="log.log",format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class SnsWrapper:
    """Encapsulates Amazon SNS topic and subscription functions."""

    def __init__(self, sns_resource) -> None:
        """
        :param sns_resource: A Boto3 Amazon SNS resource.
        """
        self.sns_resource = sns_resource


    def create_topic(self, name) -> object:
        """
        Creates a notification topic.

        :param name: The name of the topic to create.
        :return: The newly created topic.
        """
        try:
            topic = self.sns_resource.create_topic(Name=name)
  
            logger.info("Created topic %s with ARN %s.", name, topic["TopicArn"])
        except ClientError:
            logger.exception("Couldn't create topic %s.", name)
            raise
        else:
            return topic

    def list_topics(self):
        """
        Lists topics for the current account.

        :return: An iterator that yields the topics.
        """
        try:
            topics_iter = self.sns_resource.list_topics()
            logger.info("Got topics.")
        except ClientError:
            logger.exception("Couldn't get topics.")
            raise
        else:
            return topics_iter

    @staticmethod
    def subscribe(client, topic, protocol, endpoint):
        """
        Subscribes an endpoint to the topic. Some endpoint types, such as email,
        must be confirmed before their subscriptions are active. When a subscription
        is not confirmed, its Amazon Resource Number (ARN) is set to
        'PendingConfirmation'.

        :param topic: The topic to subscribe to.
        :param protocol: The protocol of the endpoint, such as 'sms' or 'email'.
        :param endpoint: The endpoint that receives messages, such as a phone number
                         (in E.164 format) for SMS messages, or an email address for
                         email messages.
        :return: The newly added subscription.
        """
        try:
            subscription = client.subscribe(
                TopicArn=topic,Protocol=protocol, Endpoint=endpoint, ReturnSubscriptionArn=True
            )
            logger.info("Subscribed %s %s to topic %s.", protocol, endpoint, topic)
        except ClientError:
            logger.exception(
                "Couldn't subscribe %s %s to topic %s.", protocol, endpoint, topic
            )
            raise
        else:
            return subscription
   
    @staticmethod
    def publish_multi_message(
        boto_client, topic, subject, message
    ) -> str:
        """
        Publishes a multi-format message to a topic. A multi-format message takes
        different forms based on the protocol of the subscriber. For example,
        an SMS subscriber might receive a short version of the message
        while an email subscriber could receive a longer version.

        :param topic: The topic to publish to.
        :param subject: The subject of the message.
        :param default_message: The default version of the message. This version is
                                sent to subscribers that have protocols that are not
                                otherwise specified in the structured message.
        :param sms_message: The version of the message sent to SMS subscribers.
        :param email_message: The version of the message sent to email subscribers.
        :return: The ID of the message.
        """
        try:
            # Publish a message to the topic
            response = boto_client.publish(
                TopicArn=topic,
                Message=json.dumps(message),
                Subject=subject,
                MessageStructure="json"
            )
            message_id = response["MessageId"]
            logger.info("Published multi-format message to topic %s.", topic)
        except ClientError:
            logger.exception("Couldn't publish message to topic %s.", topic)
            raise
        else:
            return message_id

    @staticmethod
    def delete_topic(client, topic_arn) -> None:
        """
        Deletes a topic. All subscriptions to the topic are also deleted.
        """
        try:
            client.delete_topic(TopicArn=topic_arn)
            logger.info("Deleted topic %s.", topic_arn)
        except ClientError:
            logger.exception("Couldn't delete topic %s.", topic_arn)
            raise
        else:
            return topic_arn

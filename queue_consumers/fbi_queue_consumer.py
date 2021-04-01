# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '01 Apr 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler import QueueHandler
import logging
from rabbit_fbi_elastic_indexer.handlers import FBSUpdateHandler, FastFBSUpdateHandler

logger = logging.getLogger()


class FBSQueueConsumer(QueueHandler):
    """
    Provides the callback function for the FBS scanning
    """
    def callback(self, ch, method, properties, body, connection):
        """
        Callback to run during basic consume routine.
        Arguments provided by pika standard message callback method
        :param ch: Channel
        :param method: pika method
        :param properties: pika header properties
        :param body: Message body
        :param connection: Pika connection
        """

        try:
            message = self.decode_message(body)

        except IndexError:
            # Acknowledge message if the message is not compliant
            self.acknowledge_message(ch, method.delivery_tag, connection)
            return

        try:
            if message.action in ['DEPOSIT', 'REMOVE']:
                self.queue_handler.process_event(message)

            # Acknowledge message
            self.acknowledge_message(ch, method.delivery_tag, connection)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: {message}', exc_info=e)
            raise


class SlowFBSConsumer(FBSQueueConsumer):
    """
    Uses the full FBS update handler, reading the filesystem
    """
    HANDLER_CLASS = FBSUpdateHandler


class FastFBSConsumer(FBSQueueConsumer):
    """
    Uses the lightweight handler which only uses the message as a truth source
    """
    HANDLER_CLASS = FastFBSUpdateHandler

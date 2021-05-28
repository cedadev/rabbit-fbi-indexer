# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '01 Apr 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.index_tools import CedaFbi
from rabbit_indexer.index_updaters.base import UpdateHandler
from elasticsearch.helpers import BulkIndexError
from fbs.proc.file_handlers.handler_picker import HandlerPicker
from fbs.proc.common_util.util import LDAPIdentifier
import os

# Typing imports
from typing import TYPE_CHECKING
from typing import Dict, Tuple, List
if TYPE_CHECKING:
    from rabbit_indexer.queue_handler.queue_handler import IngestMessage
    from rabbit_indexer.utils.yaml_config import YamlConfig


class FBIUpdateHandler(UpdateHandler):
    """
    Class to handle the live updates of the FBI index using events from
    rabbitMQ.
    """

    def __init__(self, conf: 'YamlConfig', **kwargs):

        # Add extra instance attributes
        self.index_updater = None
        self.handler_factory = None

        super().__init__(conf, **kwargs)

    def setup_extra(self, refresh_interval: int = 30, **kwargs):
        super().setup_extra(refresh_interval, **kwargs)

        self.handler_factory = self.load_handlers()

        # Initialise the Elasticsearch connection
        self.index_updater = CedaFbi(
            index=self.conf.get('files_index', 'name'),
            **{'headers': {
                'x-api-key': self.conf.get('elasticsearch', 'es_api_key')
            },
                'retry_on_timeout': True,
                'timeout': 30
            }
        )
        ldap_hosts = self.conf.get('ldap_configuration', 'hosts')
        self.ldap_interface = LDAPIdentifier(server=ldap_hosts, auto_bind=True)

    @staticmethod
    def load_handlers() -> 'HandlerPicker':
        """
        Load the handlers.
        Can be overridden to remove this step from the fast queue handler.

        :return: HandlerPicker
        """

        return HandlerPicker()

    def process_event(self, message: 'IngestMessage') -> None:
        """
        Processing the message according to the action within the message

        :param message: The parsed rabbitMQ message
        """

        self.logger.info(f'{message.filepath}:{message.action}')

        # Check to see if enough time has elapsed to update the mapping
        self._update_mappings()

        if message.action == 'DEPOSIT':
            self._process_deposits(message)

        elif message.action == 'REMOVE':
            self._process_deletions(message.filepath)

    def _process_deposits(self, message: 'IngestMessage') -> None:
        """
        Take the given file path and add it to the FBI index

        :param message: The parsed rabbitMQ message
        """

        # Check if path exists and has had sufficient time to appear
        self._wait_for_file(message)
        path = message.filepath

        handler = self.handler_factory.pick_best_handler(path)

        calculate_md5 = self.conf.get('files_index', 'calculate_md5')
        scan_level = self.conf.get('files_index', 'scan_level')

        if handler is not None:
            handler_instance = handler(path, scan_level, calculate_md5=calculate_md5)
            doc = handler_instance.get_metadata()

            if doc is not None:

                spot = self.pt.spots.get_spot(path)

                if spot is not None:
                    doc[0]['info']['spot_name'] = spot

                # Replace the UID and GID with name and group
                uid = doc[0]['info']['user']
                gid = doc[0]['info']['group']

                doc[0]['info']['user'] = self.ldap_interface.get_user(uid)
                doc[0]['info']['group'] = self.ldap_interface.get_group(gid)

                indexing_list = [{
                    'id': self.pt.generate_id(path),
                    'document': self._create_body(doc)
                }]

                self.index_updater.add_files(indexing_list)

    @staticmethod
    def _create_body(file_data: Tuple[Dict]) -> Dict:
        """
        Create the fbi-index document body from the handler
        response

        :param file_data: extracted metadata from the FBI file handler
        :return: FBI document body
        """

        data_length = len(file_data)

        doc = file_data[0]
        if data_length > 1:
            if file_data[1] is not None:
                doc['info']['phenomena'] = file_data[1]

            if data_length == 3:
                if file_data[2] is not None:
                    doc['info']['spatial'] = file_data[2]

        return doc

    def _process_deletions(self, path: str) -> None:
        """
        Take the given file path and delete it from the FBI index

        :param path: File path
        """

        deletion_list = [
            {'id': self.pt.generate_id(path)}
        ]
        try:
            self.index_updater.delete_files(deletion_list)
        except BulkIndexError as e:
            pass


class FastFBIUpdateHandler(FBIUpdateHandler):
    """
    Override deposit methods to provide a way to create the document
    without touching the filesystem or requiring the file to actually
    be available.
    """

    @staticmethod
    def load_handlers() -> None:
        """
        return None as not used. Reduces the load time and dependencies for the
        fast queue
        """
        return

    def process_event(self, message: 'IngestMessage') -> None:
        """
        Only use information which you can get from the message.
        Does not touch the file system

        :param message: ingest message
        """

        if message.action == 'DEPOSIT':
            self._process_deposits(message)

        elif message.action == 'REMOVE':
            self._process_deletions(message.filepath)

    def _process_deposits(self, message: 'IngestMessage') -> None:
        """
        Take the given file path and add it to the FBI index.
        This is the fast version which just uses information
        which can be gleaned from the RabbitMQ message rather
        than getting information from the file.

        :param message: IngestMessage object
        """

        doc = self._create_doc_from_message(message)

        indexing_list = [{
            'id': self.pt.generate_id(message.filepath),
            'document': self._create_body(doc)
        }]

        self.index_updater.add_files(indexing_list)

    @staticmethod
    def _create_doc_from_message(message: 'IngestMessage') -> List[Dict]:
        """
        Creates the FBI document from the rabbit message.
        Does not touch the filesystem
        :param message: IngestMessage object
        :return: document to index to elasticsearch
        """

        filename = os.path.basename(message.filepath)
        dirname = os.path.dirname(message.filepath)
        file_type = os.path.splitext(filename)[1]

        if len(file_type) == 0:
            file_type = "File without extension."

        return [{
            'info': {
                'name_auto': filename,
                'type': file_type,
                'directory': dirname,
                'size': message.filesize,
                'name': filename,
                'location': 'on_disk'
            }
        }]
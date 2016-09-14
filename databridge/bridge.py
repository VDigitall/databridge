import gevent
import logging
import requests
import functools
import couchdb
from gevent.queue import Queue, Full
from .client import APICLient
from .feed import APIRetreiver
from .helpers import check_doc, create_db_url


logger = logging.getLogger(__name__)


class APIDataBridge(object):

    def __init__(self, config):
        if not isinstance(config, dict):
            raise TypeError(
                "Expected a dict as config, got {}".format(type(config))
            )

        self.tenders_client = APICLient(
            config.get('api_key'),
            config.get('api_host'),
            config.get('api_version')
        )
        server = couchdb.Server(create_db_url(
            config.get('username', ''),
            config.get('password', ''),
            config.get('host'),
            config.get('port')
        ))
        self.db_name = config.get('db_name')

        if self.db_name not in server:
            server.create(self.db_name)
        self.db = server[self.db_name]

        filter_func = functools.partial(check_doc, self.db)

        self.retreiver = APIRetreiver(config, filter_callback=filter_func)


    def run(self):
        for item in self.retreiver.get_tenders():
            logger.info(item)



def test_run():
    bridge = APIDataBridge({
        'api_host': 'https://public.api.openprocurement.org',
        'api_version': '2',
        'api_key': '',
        'username': 'admin',
        'password': 'admin',
        'host': '127.0.0.1',
        'port': '5984',
        'db_name': 'tenders_test'
    })
    bridge.run()

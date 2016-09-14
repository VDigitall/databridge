import couchdbreq
import requests
from requests.adapters import HTTPAdapter
from databridge.helpers import create_db_url


class Storage(object):

    def __init__(self, config, session=None, adapter=None):
        if not isinstance(config, dict):
            raise TypeError(
                "Expected a dict as config, got {}".format(type(config))
            )

        server_url = create_db_url(
            config.get('username', ''),
            config.get('password', ''),
            config.get('host'),
            config.get('port')
        )
        if not session:
            session = requests.Session()
        if not adapter:
            adapter = HTTPAdapter(pool_maxsize=50, pool_connections=100)
        session.mount(server_url, adapter)
        server = couchdbreq.Server(server_url, session=session)
        self.db = server.get_or_create_db(config.get('db_name', 'tenders_db'))

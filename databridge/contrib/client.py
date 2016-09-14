import requests
from requests.adapters import HTTPAdapter
import grequests
import logging
from databridge.helpers import RetryAdapter
from ujson import loads


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class APICLient(object):

    def __init__(self, api_key, api_host, api_version, **options):

        self.base_url = "{}/api/{}".format(api_host, api_version)

        if 'session' in options:
            self.session = options.pop('session')
        else:
            self.session = requests.Session()
        self.session.auth = (api_key, '')
        self.session.headers = {"Accept": "applicaiton/json",
                                    "Content-type": "application/json"}
        resourse = options.get('resourse', 'tenders')
        self.resourse_url = "{}/{}".format(self.base_url, resourse)
        #self.session.mount(self.resourse_url, RetryAdapter)
        self.session.mount(self.resourse_url, HTTPAdapter(pool_connections=15, pool_maxsize=5))

        # retrieve cookie
        self.session.head("{}/{}".format(self.base_url, 'spore'))

    def get_tenders(self, params=None):
        if not params:
            params = {'feed': 'chages'}
        resp = self.session.get(self.resourse_url, params=params)
        if resp.ok:
            resp.close()
            return resp.json()
        else:
            logger.warn(
                'Fail while fetching tenders '
                'feed with client params: {}'.format(params)
            )
            resp.raise_for_status()

    def get_tender(self, tender_id, params=None):
        resp = self.session.get(
            "{}/{}".format(self.resourse_url, tender_id), params=params
        )
        if resp.ok:
            return resp.json()['data']
        else:
            resp.raise_for_status()

    def fetch(self, tender_ids):
        urls = ['{}/{}'.format(self.resourse_url, tender_id['id'])
                for tender_id in tender_ids]
        resp = (grequests.request('GET', url, session=self.session, stream=False)
                for url in urls)
        results = [t.json()['data'] for t in grequests.map(resp, size=5)]
        [r.close() for r in resp]
        return results

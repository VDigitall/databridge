import requests
from requests.adapters import HTTPAdapter
import grequests
import logging
from databridge.helpers import APIAdapter
from ujson import loads


logger = logging.getLogger(__name__)


class APICLient(object):

    def __init__(self, api_key, api_host, api_version, **options):

        self.base_url = "{}/api/{}".format(api_host, api_version)

        self.session = requests.Session()
        self.session.auth = (api_key, '')
        self.session.headers = {"Accept": "applicaiton/json",
                                "Content-type": "application/json"}
        resourse = options.get('resourse', 'tenders')
        self.resourse_url = "{}/{}".format(self.base_url, resourse)
        self.session.mount(self.resourse_url, APIAdapter)

        # retrieve cookie
        self.session.head("{}/{}".format(self.base_url, 'spore'))

    def get_tenders(self, params=None):
        if not params:
            params = {'feed': 'chages'}
        req = [grequests.get(self.resourse_url,
                             params=params,
                             session=self.session)]

        resp = grequests.map(req)
        for r in resp:
            if r.ok:
                return r.json()
            else:
                logger.warn(
                    'Fail while fetching tenders '
                    'feed with client params: {}'.format(params)
                )
                r.raise_for_status()

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
        resp = (grequests.get(url, session=self.session, stream=False)
                for url in urls)
        results = [t.json()['data'] for t in grequests.map(resp, size=10)]
        [r.close() for r in resp]
        return results

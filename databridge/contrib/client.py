import requests
import grequests
import logging
from .helpers import RetryAdapter


logger = logging.getLogger(__name__)


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
        self.session.mount(self.resourse_url, RetryAdapter)

        # retrieve cookie
        self.session.head("{}/{}".format(self.base_url, 'spore'))

    def get_tenders(self, params=None):
        if not params:
            params = {'feed': 'chages'}
        resp = self.session.get(self.resourse_url, params=params)
        if resp.ok:
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
            return resp.json()
        else:
            resp.raise_for_status()

    def fetch(self, tender_ids, params=None):
        urls = ['{}/{}'.format(self.resourse_url, tender_id['id'])
                for tender_id in tender_ids]
        r = (grequests.request('GET', url, session=self.session)
             for url in urls)
        return grequests.map(r, size=20)

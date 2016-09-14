import gevent
from gevent.pool import Pool
import logging
from .client import APICLient
from .storage import Storage

logger = logging.getLogger(__name__)


class Fetch(gevent.Greenlet):

    def __init__(self, src_queue, dest_queue, config):

        super(Fetch, self).__init__()
        session = config.get('session', None)
        self.client = APICLient(
            config.get('api_key', ''),
            config.get('api_host'),
            config.get('api_version'),
            session=session
        )
        self.source = src_queue
        self.dest = dest_queue

    def _run(self):
        while True:
            for feed in self.source:
                resp = self.client.fetch(feed)
                #for tender in feed:
                #    resp = self.client.get_tender(tender['id'])
                self.dest.put(resp)
                gevent.sleep(2)
            gevent.sleep(2)


class Save(gevent.Greenlet):

    def __init__(self, src_queue, dest_queue, config):

        super(Save, self).__init__()
        self.storage = config.get('storage', Storage(config))
        self.source = src_queue
        self.dest = dest_queue

    def _run(self):
        while True:
            for feed in self.source:
                for tender in feed:
                    tender['_id'] = tender['id']
                    if tender['_id'] in self.storage.db:
                        self.dest.put('Update doc; {}'.format(tender['id']))
                        doc = self.storage.db.get_doc(tender['_id'])
                        doc = tender
                        self.storage.db.save_doc(doc, batch=True)
                    else:
                        self.dest.put('Save doc; {}'.format(tender['id']))
                        self.storage.db.save_doc(tender)
        gevent.sleep(2)

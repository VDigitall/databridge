import gevent
from .client import APICLient
from .storage import Storage


class Fetch(gevent.Greenlet):

    def __init__(self, src_queue, dest_queue, config):

        super(Fetch, self).__init__()
        self.client = APICLient(
            config.get('api_key'),
            config.get('api_host'),
            config.get('api_version')
        )
        if callable(src_queue):
            self.source = src_queue
        else:
            self.source = src_queue.get
        self.dest = dest_queue

    def _run(self):
        while True:
            for feed in self.source():
                r = self.client.fetch(feed)
                self.dest.put(r)
            gevent.sleep(2)


class Save(gevent.Greenlet):

    def __init__(self, src_queue, dest_queue, config):

        super(Save, self).__init__()
        self.storage = config.get('storage', Storage(config))

        if callable(src_queue):
            self.source = src_queue
        else:
            self.source = src_queue.get
        self.dest = dest_queue

    def _run(self):
        while True:
            tenders = self.source()
            for tender in tenders:
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

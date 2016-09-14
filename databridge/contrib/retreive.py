import gevent
import random
from gevent.queue import Full
from databridge.exceptions import LBMismatchError


class RetreiverForward(gevent.Greenlet):

    def __init__(self, client, params, cookie, queue, filter_func):

        self.client = client
        self.params = params
        self.cookie = cookie
        self.queue = queue
        self.empty_response_delay = 5
        self.full_queue_delay = 3
        self.delay = 0.4
        self.flt = filter_func

    def _run(self):
        while True:
            try:
                r = self.client.get_tender(self.params)

                if self.client.session.cookies != self.cookie:
                    raise LBMismatchError

                if r['data']:
                    try:
                        self.queue.put(filter(self.flt, r['data']))
                    except Full:
                        while self.queue.full():
                            gevent.sleep(self.full_queue_delay)
                        self.queue.put(filter(self.flt, r['data']))

                    gevent.sleep(random.uniform(0, 2) * self.delay)

                else:
                    gevent.sleep(random.uniform(0, 2) *
                                 self.empty_response_delay)

                self.params['offset'] = r['next_page']['offset']

            except LBMismatchError:
                raise gevent.GreenletExit


class RetreiverBackward(gevent.Greenlet):

    def __init__(self, client, params, cookie, queue, filter_func):

        self.client = client
        self.params = params
        self.cookie = cookie
        self.queue = queue
        self.full_queue_delay = 3
        self.delay = 0.4

    def _run(self):

        r = self.client.get_tender(self.params)
        if self.client.session.cookie != self.cookie:
            raise LBMismatchError
        if r['data']:
            self.queue.put(filter(self.flt, r['data']))
        while r['data']:
            try:
                r = self.client.get_tender(self.params)

                if self.client.session.cookies != self.cookie:
                    raise LBMismatchError

                try:
                    self.queue.put(filter(self.flt, r['data']))
                except Full:
                    while self.queue.full():
                        gevent.sleep(self.full_queue_delay)
                self.queue.put(filter(self.flt, r['data']))

                gevent.sleep(random.uniform(0, 2) * self.delay)

                self.params['offset'] = r['next_page']['offset']

            except LBMismatchError:
                raise gevent.GreenletExit

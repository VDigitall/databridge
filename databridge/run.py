import argparse
import requests
import os.path
import sys
import yaml
import logging
import functools
from logging.config import dictConfig
from requests.adapters import HTTPAdapter
from .bridge import APIDataBridge
from .contrib.storage import Storage
from .contrib.workers import Fetch, Save
from .helpers import check_doc


ADAPTER = HTTPAdapter(pool_maxsize=50, pool_connections=100)
SESSION = requests.Session()
LOGGER = logging.getLogger(__name__)


def run():
    parser = argparse.ArgumentParser('API databridge')
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print "Not a valid config"
        sys.exit(1)

    with open(args.config) as cfg:
        config = yaml.load(cfg)
    if 'logging' in config:
        dictConfig(config['logging'])
    else:
        logging.basicConfig(level=logging.INFO)
    storage = Storage(config, session=SESSION, adapter=ADAPTER)
    filter_func = functools.partial(check_doc, storage.db)
    bridge = APIDataBridge(config, filter_feed=filter_func)
    bridge.add_worker(Fetch, config)
    bridge.add_worker(Save, config)
    bridge.run()

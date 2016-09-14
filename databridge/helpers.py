from requests.adapters import HTTPAdapter


RetryAdapter = HTTPAdapter(max_retries=5,
                           pool_connections=100,
                           pool_maxsize=50)


def create_db_url(username, passwd, host, port):
    if username and passwd:
        cr = '{}:{}@'.format(username, passwd)
    else:
        cr = ''
    return 'http://{}{}:{}/'.format(
                cr, host, port
            )


def save_or_update(db, doc):
    if doc['id'] not in db:
        doc['_id'] = doc['id']
        db.save(doc)
    else:
        old = db.get(doc['id'])
        old = doc
        db.save(old)


def check_doc(db, feed_item):
    if feed_item['id'] not in db:
        return True
    if db.get_doc(feed_item['id'])['dateModified'] < feed_item['dateModified']:
        return True
    return False

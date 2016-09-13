from requests.adapters import HTTPAdapter


RetryAdapter = HTTPAdapter(max_retries=5)


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
    if feed_item not in db:
        return True
    if db.get(feed_item['id'])['dateModified'] < feed_item['dateModified']:
        return True
    return False


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

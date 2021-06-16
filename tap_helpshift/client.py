import datetime
import enum
import requests
import singer
import time
import urllib


LOGGER = singer.get_logger()


def set_query_parameters(url, **params):
    """Given a URL, set or replace a query parameter and return the
    modified URL.
    >>> set_query_parameters('http://example.com?foo=bar&biz=baz', foo='stuff', bat='boots')
    'http://example.com?foo=stuff&biz=baz&bat=boots'
    """
    scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(query_string)

    new_query_string = ''

    for param_name, param_value in params.items():
        query_params[param_name] = [param_value]
        new_query_string = urllib.parse.urlencode(query_params, doseq=True)

    return urllib.parse.urlunsplit((scheme, netloc, path, new_query_string, fragment))


class GetType(enum.Enum):
    BASIC = 'basic'
    ANALYTICS = 'analytics'


class HelpshiftAPI:
    MAX_RETRIES = 10
    WAIT_TO_RETRY = 60  # documentation doesn't include error handling
    MIN_RESULTS = 10

    def __init__(self, config):
        api_key = config['api_key']
        subdomain = config['subdomain']
        self.auth = (api_key, '')
        self.base_url = 'https://api.helpshift.com/v1/{}'.format(subdomain)
        self.analytics_base_url = 'https://analytics.helpshift.com/v1/{}'.format(subdomain)

    def get(self, get_type, url, params=None):
        if not url.startswith('https://'):
            if get_type == GetType.BASIC:
                url = f'{self.base_url}/{url}'
            elif get_type == GetType.ANALYTICS:
                url = f'{self.analytics_base_url}/{url}'

        for num_retries in range(self.MAX_RETRIES):
            LOGGER.info(f'helpshift get request {url}')
            resp = requests.get(url, auth=self.auth, params=params)
            try:
                resp.raise_for_status()
            except requests.exceptions.RequestException:
                if resp.status_code == 429 and num_retries < self.MAX_RETRIES:
                    LOGGER.info(f'api query helpshift rate limit {resp.text}')
                    time.sleep(self.WAIT_TO_RETRY)
                elif resp.status_code >= 500 and num_retries < self.MAX_RETRIES:
                    LOGGER.info(f'api query helpshift 5xx error {resp.status_code} - {resp.text}', extra={
                        'url': url
                    })
                    time.sleep(self.WAIT_TO_RETRY)
                else:
                    raise Exception(f'helpshift query error: {resp.status_code} - {resp.text}')

            if resp and resp.status_code == 200:
                break

        return resp.json()


    def paging_get(self, url, results_key, replication_key=None, **get_args):
        next_page = 1
        total_returned = 0
        max_synced = None

        get_args = {k: v for k, v in get_args.items() if v is not None}
        if results_key == 'issues':
            get_args['sort-order'] = 'asc'

        while next_page:
            # Helpshift returns a 400 error when the number of issues
            # requested exceeds 50000. Because records are returned in asc
            # order, we can start a new request at page 1 using the last
            # updated_at time we saw.
            if next_page > 500 and results_key == 'issues':
                LOGGER.info('helpshift query exceeded 500 pages, starting loop over')
                next_page = 1
                get_args['updated_since'] = max_synced

            get_args['page'] = next_page

            data = self.get(GetType.BASIC, set_query_parameters(url, **get_args))

            total_pages = data.get('total-pages', 1)

            LOGGER.info('helpshift paging request', extra={
                'total_size': data.get('total-hits'),
                'total_pages': total_pages,
                'page': next_page,
                'results_key': results_key,
                'url': url,
                'total_returned': total_returned
            })

            for record in data[results_key]:
                total_returned += 1
                if replication_key:
                    max_synced = record[replication_key]
                yield record

            next_page = next_page + 1 if next_page < total_pages else None

    def analytics_paging_get(self, url, sync_thru):

        now = singer.utils.now()
        now = datetime.datetime.strftime(now, '%Y-%m-%dT%H:%M:%S')
        total_returned = 0
        get_args = {
            'to': now,
            'from': sync_thru,
            'timezone': 'UTC'
        }

        while True:
            data = self.get(GetType.ANALYTICS, set_query_parameters(
                url,
                **get_args
            ))
            results = data.get('results')

            if not results or len(results) < self.MIN_RESULTS:
                break

            LOGGER.info('helpshift analytics paging request', extra={
                'url': url,
                'total_returned': total_returned
            })

            for record in data['results']:
                total_returned += 1
                yield record

            next_key = data.get('next_key')
            if not next_key:
                break

            get_args['next_key'] = next_key

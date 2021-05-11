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


class HelpshiftAPI:
    MAX_RETRIES = 10
    WAIT_TO_RETY = 60  # documentation doesn't include error handling

    def __init__(self, config):
        api_key = config['api_key']
        subdomain = config['subdomain']
        self.auth = (api_key, '')
        self.base_url = 'https://api.helpshift.com/v1/{}'.format(subdomain)

    def get(self, url, params=None):
        if not url.startswith('https://'):
            url = f'{self.base_url}/{url}'

        for num_retries in range(self.MAX_RETRIES):
            LOGGER.info(f'helpshift get request {url}')
            resp = requests.get(url, auth=self.auth, params=params)
            try:
                resp.raise_for_status()
            except requests.exceptions.RequestException:
                if resp.status_code == 429 and num_retries < self.MAX_RETRIES:
                    LOGGER.info('api query helpshift rate limit')
                    time.sleep(self.WAIT_TO_RETRY)
                elif resp.status_code >= 500 and num_retries < self.MAX_RETRIES:
                    LOGGER.info('api query helpshift 5xx error', extra={
                        'url': url
                    })
                    time.sleep(10)
                else:
                    raise Exception(f'helpshift query error: {resp.status_code}')

            if resp and resp.status_code == 200:
                break

        return resp.json()

    def paging_get(self, url, results_key, **get_args):
        next_page = 1
        total_returned = 0

        get_args = {k: v for k, v in get_args.items() if v is not None}

        while next_page:
            get_args['page'] = next_page
            data = self.get(set_query_parameters(url, **get_args))

            if next_page == 1:
                total_size = data.get('total-hits')
                total_pages = data.get('total-pages')

            LOGGER.info('helpshift paging request', extra={
                'total_size': total_size,
                'total_pages': total_pages,
                'page': next_page,
                'results_key': results_key,
                'url': url,
                'total_returned': total_returned
            })

            for record in data[results_key]:
                total_returned += 1
                yield record

            next_page = next_page + 1 if next_page < total_pages else None

import requests
import singer
import time


LOGGER = singer.get_logger()


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

import asyncio
import datetime
import enum
import random
import time
import urllib

import aiohttp
import singer


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
        if not isinstance(param_value, (list, tuple, set)):
            param_value = [param_value]
        query_params[param_name] = param_value
        new_query_string = urllib.parse.urlencode(query_params, doseq=True)

    return urllib.parse.urlunsplit((scheme, netloc, path, new_query_string, fragment))


class GetType(enum.Enum):
    BASIC = 'basic'
    ANALYTICS = 'analytics'


class HelpshiftAPI:
    MAX_RETRIES = 10
    WAIT_TO_RETRY = 5  # documentation doesn't include error handling
    MIN_RESULTS = 10

    def __init__(self, session, config, parallel_requests=500):
        self.running = asyncio.Event()
        self.running.set()
        self.req_semaphore = asyncio.BoundedSemaphore(parallel_requests)

        self.session = session
        subdomain = config['subdomain']
        self.base_url = 'https://api.helpshift.com/v1/{}'.format(subdomain)
        self.analytics_base_url = 'https://analytics.helpshift.com/v1/{}'.format(subdomain)

    async def pause(self, seconds):
        if self.running.is_set():
            LOGGER.info('Pausing requests for %r seconds', seconds)
            self.running.clear()
            await asyncio.sleep(seconds)
            self.running.set()
            LOGGER.info('Requests unpaused')

    async def get(self, get_type, url, params=None):
        if not url.startswith('https://'):
            if get_type == GetType.BASIC:
                url = f'{self.base_url}/{url}'
            elif get_type == GetType.ANALYTICS:
                url = f'{self.analytics_base_url}/{url}'

        for num_retries in range(self.MAX_RETRIES + 1):
            rate_limited = False
            async with self.req_semaphore:
                await self.running.wait()

                async def _get_resp_text(resp):
                    """ Dumb method to funnel resp.text call exceptions """
                    try:
                        return await resp.text()
                    except:
                        return ''

                LOGGER.info(f'helpshift get request {url}')
                resp = None

                try:
                    resp = await self.session.get(url, params=params)
                    resp.raise_for_status()
                    return await resp.json()

                except (aiohttp.client_exceptions.ClientResponseError,
                        aiohttp.client_exceptions.ClientPayloadError) as exc:
                    if resp.status == 429 and num_retries < self.MAX_RETRIES:
                        rate_limited = True
                        rate_limit_msg = _get_resp_text(resp.text)
                        LOGGER.info(
                            f'api query helpshift rate limit: {rate_limit_msg}', extra={
                                'url': url
                            }
                        )
                    elif resp.status >= 500 and num_retries < self.MAX_RETRIES:
                        error_msg = _get_resp_text(resp)
                        LOGGER.info(
                            f'api query helpshift 5xx error {resp_status}', extra={
                                'url': url
                            }
                        )

                except (aiohttp.client_exceptions.ServerDisconnectedError,
                        aiohttp.client_exceptions.ClientConnectionError,
                        RuntimeError) as exc:
                    if resp:
                        error_msg = _get_resp_text(resp)
                        LOGGER.info(
                            f'api query helpshift connection error {resp.status}', extra={
                                'url': url
                            }
                        )

                except (aiohttp.ClientError) as exc:
                    raise Exception(f'helpshift query error: {exc}')

            if rate_limited:
                # We're rate limited, make all requests wait
                await self.pause(self.WAIT_TO_RETRY * num_retries)
            else:
                # This particular request had an error, so only it will wait
                await asyncio.sleep(self.WAIT_TO_RETRY * num_retries)

    async def paging_get(self, url, results_key, replication_key=None, **get_args):
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

            data = await self.get(GetType.BASIC, set_query_parameters(url, **get_args))

            total_pages = data.get('total-pages', 1)

            LOGGER.info('helpshift paging request: %r', {
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

        LOGGER.info('helpshift paging request complete: %r', {
            'results_key': results_key,
            'last_url': url,
            'total_returned': total_returned
        })

    async def analytics_paging_get(self, url, from_, issue_id=None):
        now = singer.utils.now()
        # Timezone info needs to match `now` so we can compare without error.
        from_ = from_.replace(tzinfo=now.tzinfo)

        # Querying for a span of >180 days fails with a 400
        max_timedelta = datetime.timedelta(days=180)
        periods = []
        while now - from_ > max_timedelta:
            next_from = from_ + max_timedelta
            periods.append((from_, next_from))
            from_ = next_from
        if from_ < now:
            periods.append((from_, now))

        total_returned = 0

        date_fmt = '%Y-%m-%dT%H:%M:%S'
        for from_, to in periods:
            get_args = {
                'from': from_.strftime(date_fmt),
                'to': to.strftime(date_fmt),
                'timezone': 'UTC',
                'limit': 2000,
                'includes': [
                    'human_ttfr',
                    'first_human_responder_id'
                ]
            }
            if issue_id:
                get_args['id'] = issue_id

            while True:
                url = set_query_parameters(
                    url,
                    **get_args
                )

                data = await self.get(GetType.ANALYTICS, url)
                results = data.get('results')

                LOGGER.info('helpshift analytics paging request', extra={
                    'url': url,
                    'total_returned': total_returned
                })

                for record in data['results']:
                    total_returned += 1
                    yield record

                if not results or len(results) < self.MIN_RESULTS:
                    break

                next_key = data.get('next_key')
                if not next_key:
                    break

                get_args['next_key'] = next_key

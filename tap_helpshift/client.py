import asyncio
import datetime
import enum
import random
import statistics
import time
import urllib
import re

import aiohttp
import singer
import dateutil.parser


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


class QueryStats:
    log_size = 1000

    def __init__(self):
        self._requests = []

    def record(self, url, status):
        self._requests.append({
            'url': url,
            'status': status,
            'at': time.monotonic()
        })

    def recovery_stats(self):
        # High level questions:
        # - How long should we wait after a rate limit error?
        # - How long should we wait after a 500 error?
        # - How many concurrent requests should we use (want to maximize throughput)?

        all_periods = {
            'success': [None],
            'disconnect': [None],
            'rate_limit': [None],
            'error': [None],
        }

        def extend_period(periods, request):
            for _periods in all_periods.values():
                if _periods is not periods:
                    close_last_period(_periods)
            if periods[-1] is None:
                periods[-1] = (request['at'], request['at'])
            else:
                periods[-1] = (periods[-1][0], request['at'])

        def close_last_period(periods):
            if periods[-1] is not None:
                periods.append(None)

        for request in self._requests:
            if request['status'] == 0:
                extend_period(all_periods['disconnect'], request)
            elif request['status'] >= 200 and request['status'] < 400:
                extend_period(all_periods['success'], request)
            elif request['status'] == 429:
                extend_period(all_periods['rate_limit'], request)
            else:
                extend_period(all_periods['error'], request)

        stats = {}
        for key, periods in all_periods.items():
            if periods:
                durations = [p[1] - p[0] for p in periods if p is not None]
                if not durations:
                    continue
                stats[key] = {
                    'min': min(durations),
                    'max': max(durations),
                    'avg': statistics.mean(durations),
                    'median': statistics.median(durations),
                }
        return stats


class HelpshiftAPI:
    MAX_RETRIES = 8
    WAIT_TO_RETRY = 20  # documentation doesn't include error handling
    MIN_WAIT = 5
    MIN_WAIT_RATE_LIMIT = 120
    MIN_RESULTS = 10

    def __init__(self, session, config, parallel_requests=25):
        self.running = asyncio.Event()
        self.running.set()
        self.req_semaphore = asyncio.BoundedSemaphore(parallel_requests)
        self.stats = QueryStats()

        self.session = session
        subdomain = config['subdomain']
        self.base_url = 'https://api.helpshift.com/v1/{}'.format(subdomain)
        self.analytics_base_url = 'https://analytics.helpshift.com/v1/{}'.format(subdomain)

    async def global_pause(self, seconds):
        if self.running.is_set():
            LOGGER.info('Pausing all requests for %r seconds', seconds)
            LOGGER.info('Recovery stats: %r', self.stats.recovery_stats())
            self.running.clear()
            task = asyncio.create_task(asyncio.sleep(seconds))
            self._global_pause_task = task
            await task
            if self._global_pause_task == task:
                self.running.set()
                LOGGER.info('Requests unpaused')

    async def get(self, get_type, url, params=None):
        if not url.startswith('https://'):
            if get_type == GetType.BASIC:
                url = f'{self.base_url}/{url}'
            elif get_type == GetType.ANALYTICS:
                url = f'{self.analytics_base_url}/{url}'

        for num_retries in range(self.MAX_RETRIES + 1):
            pause = False
            wait_s = self.MIN_WAIT
            async with self.req_semaphore:
                await self.running.wait()

                async def _get_resp_text(resp):
                    """ Dumb method to funnel resp.text call exceptions """
                    try:
                        text = await resp.text()
                        return str(text)
                    except:
                        return ''

                def _get_wait_time(stats_key):
                    stats = self.stats.recovery_stats()
                    wait_s = stats.get(stats_key, {}).get('median', 0)
                    return max(wait_s, self.MIN_WAIT)

                # current = asyncio.current_task()
                # LOGGER.info(f'{current.get_name()} helpshift get request {url}')
                err_message = None
                status = 0
                wait_s = 0

                try:
                    async with self.session.get(url, params=params) as resp:
                        if resp.status >= 200:
                            status = resp.status
                        if not status or status >= 400:
                            err_message = await _get_resp_text(resp)
                        resp.raise_for_status()
                        self.stats.record(url, status)
                        return await resp.json()

                except (aiohttp.client_exceptions.ClientResponseError,
                        aiohttp.client_exceptions.ClientPayloadError) as exc:
                    self.stats.record(url, status)

                    if status == 429 and num_retries < self.MAX_RETRIES:
                        pause = True

                        match = re.search(r'retry after (.*) UTC', err_message or '')
                        if match:
                            # If we get a request to retry after a certain time, we'll respect it.
                            retry_after = dateutil.parser.parse(match.group(1))
                            now = datetime.datetime.utcnow().replace(tzinfo=retry_after.tzinfo)
                            wait_s = (retry_after - now).total_seconds()
                            LOGGER.debug('retry after %r', retry_after)
                        else:
                            wait_s = _get_wait_time('rate_limit')

                        LOGGER.info(
                            f'api query helpshift rate limit: {err_message}', extra={
                                'url': url
                            }
                        )
                    elif status >= 500 and num_retries < self.MAX_RETRIES:
                        wait_s = _get_wait_time('error')
                        LOGGER.info(
                            f'api query helpshift 5xx error {status}: {err_message}', extra={
                                'url': url
                            }
                        )

                except (aiohttp.client_exceptions.ServerDisconnectedError,
                        aiohttp.client_exceptions.ClientConnectionError,
                        RuntimeError) as exc:
                    pause = True
                    self.stats.record(url, 0)
                    wait_s = _get_wait_time('disconnect')
                    LOGGER.info(
                        f'api query helpshift connection error {status}: {err_message}', extra={
                            'url': url
                        }
                    )

                except (aiohttp.ClientError) as exc:
                    raise Exception(f'helpshift query error: {exc}')

            wait_s = max(wait_s, self.MIN_WAIT)
            if pause:
                # wait_s = max(wait_s, self.MIN_WAIT_RATE_LIMIT)
                # We're rate limited, make all requests wait
                await self.global_pause(wait_s)
            elif wait_s:
                # This particular request had an error, so only it will wait
                await asyncio.sleep(wait_s)

    async def paging_get(self, url, results_key, replication_key=None, **get_args):
        next_page = 1
        total_returned = 0
        max_synced = None

        get_args = {k: v for k, v in get_args.items() if v is not None}
        if results_key == 'issues':
            get_args['sort-order'] = 'asc'

        if 'page-size' not in get_args:
            get_args['page-size'] = 1000

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

        request_args = []
        if issue_id:
            request_args.append({
                'id': issue_id,
                'timezone': 'UTC',
                'includes': [
                    'human_ttfr',
                    'first_human_responder_id'
                ]
            })
        else:
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
                request_args.append({
                    'from': from_.strftime(date_fmt),
                    'to': to.strftime(date_fmt),
                    'timezone': 'UTC',
                    'limit': 2000,
                    'includes': [
                        'human_ttfr',
                        'first_human_responder_id'
                    ]
                })

        for get_args in request_args:
            if issue_id:
                get_args['id'] = issue_id

            while True:
                url = set_query_parameters(
                    url,
                    **get_args
                )

                data = await self.get(GetType.ANALYTICS, url)
                if not data:
                    break
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

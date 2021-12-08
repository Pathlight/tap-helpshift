from contextlib import asynccontextmanager
import asyncio
import datetime
import enum
import random
import re
import time
import urllib
import random

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


class RateLimitedError(Exception):
    pass


class RateLimiter:
    LIMIT = 1500
    PERIOD = 60  # 1 minute

    def __init__(self, allowance=1, concurrent_allowance=1):
        self._request_timestamps = []
        self.limit = int(self.LIMIT * allowance)
        self.semphore = asyncio.BoundedSemaphore(int(self.limit * concurrent_allowance))

    def _clean_request_timestamps(self, now):
        while self._request_timestamps and now - self._request_timestamps[0] >= self.PERIOD:
            self._request_timestamps.pop(0)

    async def try_acquire(self, now):
        self._clean_request_timestamps(now)
        self.check()
        self._request_timestamps.append(now)

    async def rate_limited(self, until=None):
        # We got a rate limit we didn't expect. Since we don't own the api
        # key, probably someone else is using it to make requests. Update
        # _request_timestamps to "know" about these requests so the rate
        # limiter works more smoothly.
        now = time.monotonic()
        if until:
            now_dt = datetime.datetime.utcnow().replace(tzinfo=until.tzinfo)
            now = now + (until - now_dt).total_seconds()
        else:
            now = time.monotonic()
        LOGGER.info(f'unexpected rate limit: updating state')
        self._clean_request_timestamps(now)
        while len(self._request_timestamps) < self.limit:
            self._request_timestamps.append(now)
        self._request_timestamps.sort()

    def check(self):
        if len(self._request_timestamps) >= self.limit:
            raise RateLimitedError

    @asynccontextmanager
    async def acquire(self):
        async with self.semphore:
            # Stagger all requests slightly. This helps us avoid sending
            # too many requests to helpshift when we would get a rate limit
            # response back. This way, the rate limited method will have a
            # chance to update the internal state before more requests are run.
            await asyncio.sleep(random.random())
            while True:
                now = time.monotonic()
                try:
                    await self.try_acquire(now)
                    break
                except RateLimitedError:
                    # Wait until the next request would be cleaned up
                    wait_s = self.PERIOD - (now - self._request_timestamps[0])
                    LOGGER.info(f'rate limited: checking again in {wait_s} seconds')
                    await asyncio.sleep(wait_s)
            yield


class HelpshiftAPI:
    MAX_RETRIES = 8
    MIN_RESULTS = 10

    RATE_LIMIT_WAIT = 60
    MIN_WAIT = 5
    RATE_LIMIT_PER_MINUTE = 1500

    def __init__(self, session, config):
        self.running = asyncio.Event()
        self.running.set()
        self.rate_limiter = RateLimiter(
            allowance=config.get('rate_limit_allowance_percent', 1),
            concurrent_allowance=config.get('rate_limit_concurrent_allowance_percent', 1))

        self.session = session
        subdomain = config['subdomain']
        self.base_url = 'https://api.helpshift.com/v1/{}'.format(subdomain)
        self.analytics_base_url = 'https://analytics.helpshift.com/v1/{}'.format(subdomain)

    async def global_pause(self, seconds):
        if self.running.is_set():
            LOGGER.info('Pausing all requests for %r seconds', seconds)
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
            async with self.rate_limiter.acquire():
                await self.running.wait()

                async def _get_resp_text(resp):
                    """ Dumb method to funnel resp.text call exceptions """
                    try:
                        text = await resp.text()
                        return str(text)
                    except:
                        return ''

                err_message = None
                status = 0
                wait_s = 0

                try:
                    LOGGER.info('GET %s %r', url, params)
                    async with self.session.get(url, params=params) as resp:
                        if resp.status >= 200:
                            status = resp.status
                        if not status or status >= 400:
                            err_message = await _get_resp_text(resp)
                        resp.raise_for_status()
                        return await resp.json()

                except Exception as exc:
                    # Ensure we log error info so we can diagnose any error handling issues more quickly.
                    if num_retries >= self.MAX_RETRIES:
                        raise

                    if isinstance(exc, (aiohttp.client_exceptions.ClientResponseError, aiohttp.client_exceptions.ClientPayloadError, aiohttp.client_exceptions.ClientResponseError)):
                        if status == 429:
                            match = re.search(r'retry after (.*) UTC', err_message or '')
                            retry_after = None
                            if match:
                                # If we get a request to retry after a certain time, we'll respect it.
                                try:
                                    retry_after = dateutil.parser.parse(match.group(1))
                                except (TypeError, ValueError):
                                    pass

                            await self.rate_limiter.rate_limited(until=retry_after)
                            if retry_after:
                                now = datetime.datetime.utcnow().replace(tzinfo=retry_after.tzinfo)
                                wait_s = (retry_after - now).total_seconds()
                                LOGGER.debug('retry after %r', retry_after)

                            LOGGER.debug(
                                f'api query helpshift rate limit: {err_message}', extra={
                                    'url': url
                                }
                            )

                        elif status >= 500:
                            LOGGER.info(
                                f'api query helpshift error {status}: {err_message}', extra={
                                    'url': url
                                }
                            )

                        else:
                            raise

                    elif isinstance(exc, asyncio.TimeoutError):
                        # Retry on timeout
                        LOGGER.info(
                            f'api query helpshift error {status}: {exc}', extra={
                                'url': url
                            }
                        )

                    elif isinstance(exc, (aiohttp.client_exceptions.ServerDisconnectedError, aiohttp.client_exceptions.ClientConnectionError, RuntimeError)):
                        pause = True
                        LOGGER.info(
                            f'api query helpshift connection error {status}: {err_message}', extra={
                                'url': url
                            }
                        )

                    else:
                        raise

                wait_s = max(wait_s, self.MIN_WAIT)
                if pause:
                    # We're rate limited, make all requests wait
                    await self.global_pause(wait_s)
                elif wait_s:
                    # This particular request had an error, so only it will wait
                    await asyncio.sleep(wait_s)

        assert False, 'unreachable'

    async def paging_get(self, url, results_key, replication_key=None, **get_args):
        MAX_NUM_RESULTS = 50000
        next_page = 1
        total_returned = 0
        max_synced = None

        get_args = {k: v for k, v in get_args.items() if v is not None}
        if results_key == 'issues':
            get_args['sort-order'] = 'asc'

        if 'page-size' not in get_args:
            get_args['page-size'] = 1000

        while next_page:
            # Helpshift returns a 400 error when the number of results
            # requested exceeds 50000. Because records are returned in asc
            # order, we can start a new request at page 1 using the last
            # updated_at time we saw.
            if next_page > MAX_NUM_RESULTS / get_args['page-size']:
                LOGGER.info(f'helpshift query exceeded {next_page-1} pages, starting loop over')
                next_page = 1
                get_args['updated_since'] = max_synced

            get_args['page'] = next_page

            page_url = set_query_parameters(url, **get_args)
            data = await self.get(GetType.BASIC, page_url)
            if not data:
                raise RuntimeError(f'No response for {page_url}')

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
        now = singer.utils.now()
        # Timezone info needs to match `now` so we can compare without error.
        from_ = from_.replace(tzinfo=now.tzinfo)

        # Querying for a span of >180 days fails with a 400
        max_timedelta = datetime.timedelta(days=180)
        periods = []
        if issue_id:
            # Helpshift has asked us to make these requests with a 1-second
            # range in this manner to avoid making expensive requests from
            # their servers.
            periods.append((from_, from_ + datetime.timedelta(seconds=1)))
        else:
            # Take the full range of time we need to request for and break
            # it up into 180-day chunks (as 180 days is the largest time
            # range the Helpshift API will accept for this request).
            while now - from_ > max_timedelta:
                next_from = from_ + max_timedelta
                periods.append((from_, next_from))
                from_ = next_from
            if from_ < now:
                periods.append((from_, now))

        total_returned = 0

        date_fmt = '%Y-%m-%dT%H:%M:%S'
        for from_, to in periods:
            args = {
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
                args['id'] = issue_id
                args['limit'] = 1
            request_args.append(args)

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

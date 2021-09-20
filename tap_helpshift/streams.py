import concurrent.futures
import datetime
import pytz
import queue

from singer.utils import strftime as singer_strftime
import singer

LOGGER = singer.get_logger()

SUB_STREAMS = {
    'issues': ['messages', 'issue_analytics']
}


def iso_format(date_value):
    if isinstance(date_value, str):
        return date_value
    value = datetime.datetime.utcfromtimestamp(date_value/1000.0)
    value = datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S')
    return value


class Stream():
    name = None
    url = None
    replication_method = None
    key_properties = None
    stream = None
    datetime_fields = None
    date_format = None

    def __init__(self, client=None, start_date=None, executor=None, q=None):
        self.client = client
        if start_date:
            self.start_date_int = start_date
            self.start_date = datetime.datetime.fromtimestamp(start_date / 1000)
        else:
            # No need to go further back than 2011, the year Helpshift was founded.
            self.start_date = datetime.datetime(2011, 1, 1)
            self.start_date_int = int(self.start_date.strftime('%s')) * 1000

        self.executor = executor
        self.q = q

    def is_selected(self):
        return self.stream is not None

    def update_bookmark(self, state, value):
        current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
        if value and value > current_bookmark:
            self.q.put({'write_bookmark': (state, self.name, self.replication_key, value)})

    def transform_value(self, key, value):
        if key in self.datetime_fields and value:
            value = datetime.datetime.utcfromtimestamp(value/1000.0).replace(tzinfo=pytz.utc)
            # reformat to use RFC3339 format
            value = singer_strftime(value)

        return value


class Issues(Stream):
    name = 'issues'
    url = 'issues'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'updated_at'  # TODO
    datetime_fields = set(['updated_at', 'created_at'])
    results_key = 'issues'

    def sync(self, state):
        try:
            sync_thru = singer.get_bookmark(state, self.name, self.replication_key)
        except TypeError:
            sync_thru = self.start_date_int

        curr_synced_thru = max(sync_thru, self.start_date_int)

        messages_stream = Messages(self.client)
        analytics_stream = IssueAnalytics(self.client)

        records =  self.client.paging_get(
            self.url,
            self.results_key,
            self.replication_key,
            includes='["custom_fields", "meta"]',
            updated_since=curr_synced_thru
        )

        q = queue.Queue()

        def handle_row_thread(row, q):
            if messages_stream.is_selected() and row.get('messages'):
                for item in messages_stream.sync(row):
                    q.put(item)

            if analytics_stream.is_selected():
                for item in analytics_stream.sync(row):
                    q.put(item)

        def consume_q():
            # Yield items from threads
            while True:
                try:
                    yield q.get_nowait()
                except queue.Empty:
                    return

        futures = []

        def clean_futures():
            for fut in futures:
                if fut.done():
                    futures.remove(fut)

        for row in records:
            clean_futures()
            yield from consume_q()

            record = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield(self.stream, record)
            curr_synced_thru = max(curr_synced_thru, row[self.replication_key])

            futures.append(self.executor.submit(handle_row_thread, row, q))

        for fut in concurrent.futures.as_completed(futures):
            yield from consume_q()
            exc = fut.exception()
            if exc:
                raise exc

        self.update_bookmark(state, curr_synced_thru)


class Messages(Stream):
    name = 'messages'
    replication_method = 'INCREMENTAL'
    key_properties = ['id']
    replication_key = 'created_at'
    datetime_fields = set(['created_at'])

    def sync(self, issue):
        for row in issue.get('messages'):
            record = {k: self.transform_value(k, v) for (k, v) in row.items()}
            record['issue_id'] = issue['id']
            yield(self.stream, record)


class Apps(Stream):
    name = 'apps'
    url = 'apps'
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    results_key = 'apps'

    def sync(self, state):
        for row in self.client.paging_get(self.url, self.results_key):
            yield(self.stream, row)


class Agents(Stream):
    name = 'agents'
    url = 'agents'
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    results_key = 'profiles'

    def sync(self, state):
        for row in self.client.paging_get(self.url, self.results_key):
            yield(self.stream, row)


class IssueAnalytics(Stream):
    name = 'issue_analytics'
    url = 'analytics/issue'
    key_properties = ['row_id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'

    def sync(self, issue):
        created_at = datetime.datetime.fromtimestamp(issue['created_at'] / 1000)
        for row in self.client.analytics_paging_get(
            self.url,
            from_=created_at,
            issue_id=issue['id']
        ):
            yield (self.stream, row)


STREAMS = {
    'issues': Issues,
    'messages': Messages,
    'agents': Agents,
    'apps': Apps,
    'issue_analytics': IssueAnalytics
}

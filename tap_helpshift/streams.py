import datetime
import pytz
import singer

from singer.utils import strftime as singer_strftime

LOGGER = singer.get_logger()

SUB_STREAMS = {
    'issues': ['messages']
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

    def __init__(self, client=None, start_date=None):
        self.client = client
        if start_date:
            self.start_date = start_date
        else:
            self.start_date = datetime.datetime.min.strftime('%Y-%m-%d')

    def is_selected(self):
        return self.stream is not None

    def update_bookmark(self, state, value):
        current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
        if value and value > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)

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
            sync_thru = self.start_date

        curr_synced_thru = max(sync_thru, self.start_date)

        messages_stream = Messages(self.client)

        for row in self.client.paging_get(
                                          self.url,
                                          self.results_key,
                                          self.replication_key,
                                          updated_since=curr_synced_thru
        ):
            record = {k: self.transform_value(k, v) for (k, v) in row.items()}
            yield(self.stream, record)

            if messages_stream.is_selected() and row.get('messages'):
                yield from messages_stream.sync(row)

            curr_synced_thru = max(curr_synced_thru, row[self.replication_key])

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
    key_properites = ['row_id']
    replication_method = 'INCREMENTAL'
    replication_key = 'updated_at'

    def sync(self, state):
        try:
            sync_thru = singer.get_bookmark(state, self.name, self.replication_key)
        except TypeError:
            sync_thru = self.start_date

        sync_thru = iso_format(sync_thru)
        curr_synced_thru = max(sync_thru, iso_format(self.start_date))

        for row in self.client.analytics_paging_get(
            self.url,
            sync_thru=curr_synced_thru
        ):
            yield(self.stream, row)

            curr_synced_thru = max(curr_synced_thru, row[self.replication_key])

        if curr_synced_thru > sync_thru:
            singer.write_bookmark(state, self.name, self.replication_key, curr_synced_thru)


STREAMS = {
    'issues': Issues,
    'messages': Messages,
    'agents': Agents,
    'apps': Apps,
    'issue_analytics': IssueAnalytics
}

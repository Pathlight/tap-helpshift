import datetime
import pytz
import singer

from singer.utils import strftime as singer_strftime

LOGGER = singer.get_logger()

SUB_STREAMS = {
    'issues': ['messages']
}


class Stream():
    name = None
    replication_method = None
    key_properties = None
    stream = None

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

        for row in self.client.paging_get('issues', self.results_key, updated_since=curr_synced_thru):
            for key, value in row.items():
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
    replication_method = 'created_at'
    datetime_fields = set(['created_at'])

    def sync(self, issue):
        for row in issue.get('messages'):
            record = {k: self.transform_value(k, v) for (k, v) in row.items()}
            record['issue_id'] = issue['id']
            yield(self.stream, record)


class Agents(Stream):
    name = 'agents'
    replication_method = 'FULL_TABLE'
    key_properties = ['id']
    results_key = 'profiles'

    def sync(self, state):

        for row in self.client.paging_get('agents', self.results_key):
            yield(self.stream, row)


STREAMS = {
    'issues': Issues,
    'messages': Messages,
    'agents': Agents
}

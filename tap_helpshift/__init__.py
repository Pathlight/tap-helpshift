#!/usr/bin/env python3
from collections import defaultdict
from contextlib import ExitStack
import asyncio
import copy
import json
import os
import queue
import threading
import time

from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import aiohttp
import singer
import singer.metrics as metrics

from tap_helpshift.client import HelpshiftAPI
from tap_helpshift.streams import STREAMS, SUB_STREAMS
from tap_helpshift.sync import sync_stream
from tap_helpshift.util import consume_q


REQUIRED_CONFIG_KEYS = ["start_date", "api_key", "subdomain"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


def get_selected_streams(catalog):
    selected_stream_names = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_names.append(stream.tap_stream_id)
    return selected_stream_names


def get_sub_stream_names():
    sub_stream_names = []
    for parent_stream in SUB_STREAMS:
        sub_stream_names.extend(SUB_STREAMS[parent_stream])
    return sub_stream_names


class DependencyException(Exception):
    pass


def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract {0} data. "
                "To receive {0} data, you also need to select {1}.")
    for parent_stream_name in SUB_STREAMS:
        sub_stream_names = SUB_STREAMS[parent_stream_name]
        for sub_stream_name in sub_stream_names:
            if sub_stream_name in selected_stream_ids and parent_stream_name not in selected_stream_ids:
                errs.append(msg_tmpl.format(sub_stream_name, parent_stream_name))

    if errs:
        raise DependencyException(" ".join(errs))


def populate_class_schemas(catalog, selected_stream_names):
    for stream in catalog.streams:
        if stream.tap_stream_id in selected_stream_names:
            STREAMS[stream.tap_stream_id].stream = stream


class SyncApplication:
    def __init__(self, client, catalog, config):
        self.client = client
        self.catalog = catalog

        self.start_date = config['start_date']

        self.selected_stream_names = get_selected_streams(catalog)
        validate_dependencies(self.selected_stream_names)
        populate_class_schemas(catalog, self.selected_stream_names)
        all_sub_stream_names = get_sub_stream_names()

        self.selected_streams = []
        # Output schemas and build selected streams
        for stream in catalog.streams:
            stream_name = stream.tap_stream_id
            if stream_name not in self.selected_stream_names:
                LOGGER.info("%s: Skipping - not selected", stream_name)
                continue

            # parent stream will sync sub stream
            if stream_name in all_sub_stream_names:
                continue

            self.selected_streams.append(stream)

        self.stream_name_by_task = {}
        self.stream_schedule = defaultdict(int)
        self.stream_counters = {}

    def write_schemas(self):
        for stream in self.selected_streams:
            stream_name = stream.tap_stream_id
            singer.write_schema(
                stream_name,
                stream.schema.to_dict(),
                stream.key_properties
            )

            sub_stream_names = SUB_STREAMS.get(stream_name)
            if sub_stream_names:
                for sub_stream_name in sub_stream_names:
                    if sub_stream_name not in self.selected_stream_names:
                        continue
                    sub_instance = STREAMS[sub_stream_name]
                    sub_stream = STREAMS[sub_stream_name].stream
                    sub_stream_schema = sub_stream.schema.to_dict()
                    singer.write_schema(
                        sub_stream.tap_stream_id,
                        sub_stream_schema,
                        sub_instance.key_properties
                    )

    def sync_stream_bg(self, stream_name, state, *args, start_date=None):
        start_date = start_date or self.start_date

        if stream_name not in self.stream_counters:
            self.stream_counters[stream_name] = metrics.record_counter(stream_name)
        counter = self.stream_counters[stream_name]
        instance = STREAMS[stream_name](self.client, start_date, sync_stream_bg=self.sync_stream_bg)
        task = asyncio.create_task(sync_stream(state, instance, counter, *args, start_date=start_date))
        self.stream_name_by_task[task] = stream_name

    def spawn_selected_streams(self, state):
        now = time.monotonic()
        for stream in self.selected_streams:
            stream_name = stream.tap_stream_id
            running_streams = set(self.stream_name_by_task.values())
            if stream_name in running_streams:
                continue
            if now < self.stream_schedule[stream_name]:
                continue

            self.sync_stream_bg(stream_name, state)

    async def manage_tasks(self, timeout, stream_cooldown=300):
        """
        Returns True if there are running tasks, False otherwise.
        """

        try:
            all_tasks = set(self.stream_name_by_task.keys()) | asyncio.all_tasks()
            if not all_tasks:
                # No tasks left to await, we're all done!
                return False

            for task in asyncio.as_completed(all_tasks, timeout=timeout):
                await task
                if task in self.stream_name_by_task:
                    stream_name = self.stream_name_by_task.pop(task)
                    self.stream_schedule[stream_name] = time.monotonic() + stream_cooldown
        except asyncio.TimeoutError:
            pass

        # Live to loop another day
        return True

    async def run(self, state):
        self.write_schemas()

        counters = set()
        with ExitStack() as stack:
            for counter in self.stream_counters.values():
                if counter not in counters:
                    counters.add(counter)
                    stack.enter_context(counter)

            running = True
            while running:
                self.spawn_selected_streams(state)
                running = await self.manage_tasks(timeout=1)

        singer.write_state(state)
        LOGGER.info("Finished sync")


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        key_properties = ['id']
        valid_replication_keys = None
        if stream_id == 'issues':
            valid_replication_keys = ['updated_at']
        elif stream_id == 'messages':
            valid_replication_keys = ['created_at']
        stream_metadata = metadata.get_standard_metadata(
            schema=schema.to_dict(),
            key_properties=key_properties,
            valid_replication_keys=valid_replication_keys,
            replication_method=None
        )
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


async def aiomain():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        auth = aiohttp.BasicAuth(args.config['api_key'])
        async with aiohttp.ClientSession(auth=auth) as session:
            client = HelpshiftAPI(session, args.config)
            app = SyncApplication(client, catalog, args.config)
            await app.run(args.state)


@utils.handle_top_exception(LOGGER)
def main():
    asyncio.run(aiomain())


if __name__ == "__main__":
    main()

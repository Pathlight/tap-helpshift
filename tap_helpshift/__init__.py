#!/usr/bin/env python3
import concurrent.futures
import json
import os
import queue
import singer

from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

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


def writer_thread(writer_q):
    LOGGER.info(f'hello from writer_thread')
    while True:
        try:
            resp = writer_q.get()
            if resp is None:
                return

            write_state = resp.get('write_state')
            write_bookmark = resp.get('write_bookmark')
            write_record = resp.get('write_record')

            if write_state:
                singer.write_state(*write_state)

            if write_record:
                singer.write_record(*write_record)

            if write_bookmark:
                singer.write_bookmark(*write_bookmark)
        except:
            LOGGER.exception('Writer thread had an exception')


def sync_stream_thread(state, stream_name, client, start_date, config, executor, writer_q, task_q, *args):
    try:
        LOGGER.info("%s: Starting sync", stream_name)
        instance = STREAMS[stream_name](client, start_date, executor, writer_q, task_q)
        counter_value = sync_stream(state, start_date, instance, config, writer_q, *args)
        writer_q.put({'state': state})
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)
    except:
        LOGGER.exception('Exception syncing stream %s', stream_name)


def do_sync(client, catalog, state, config):
    start_date = config['start_date']

    selected_stream_names = get_selected_streams(catalog)
    validate_dependencies(selected_stream_names)
    populate_class_schemas(catalog, selected_stream_names)
    all_sub_stream_names = get_sub_stream_names()

    qsize = config.get('qsize', 100)
    writer_q = queue.Queue(maxsize=qsize)
    task_q = queue.Queue(maxsize=qsize)

    futures = []
    # Add one to concurrency for the writer thread.
    concurrency = config.get('concurrency', 10) + 1
    with concurrent.futures.ThreadPoolExecutor(concurrency) as executor:
        for stream in catalog.streams:
            stream_name = stream.tap_stream_id
            if stream_name not in selected_stream_names:
                LOGGER.info("%s: Skipping - not selected", stream_name)
                continue

            sub_stream_names = SUB_STREAMS.get(stream_name)

            # parent stream will sync sub stream
            if stream_name in all_sub_stream_names:
                continue

            singer.write_schema(
                stream_name,
                stream.schema.to_dict(),
                stream.key_properties
            )

            if sub_stream_names:
                for sub_stream_name in sub_stream_names:
                    if sub_stream_name not in selected_stream_names:
                        continue
                    sub_instance = STREAMS[sub_stream_name]
                    sub_stream = STREAMS[sub_stream_name].stream
                    sub_stream_schema = sub_stream.schema.to_dict()
                    singer.write_schema(
                        sub_stream.tap_stream_id,
                        sub_stream_schema,
                        sub_instance.key_properties
                    )

            fut = executor.submit(sync_stream_thread, state, stream_name, client, start_date, config, executor, writer_q, task_q)
            futures.append(fut)

        # Start the writer thread.
        writer_fut = executor.submit(writer_thread, writer_q)

        while True:
            to_consume = consume_q(task_q)
            # Don't let the list of futures we're waiting on exceed
            # qsize. Otherwise, we may submit too many tasks we won't get
            # to for some time and run out of memory.
            while len(futures) < qsize:
                task = next(to_consume, None)
                if not task:
                    break
                stream_name, *args = task
                fut = executor.submit(sync_stream_thread, state, stream_name, client, start_date, config, executor, writer_q, task_q, *args)
                futures.append(fut)

            if not futures:
                break

            try:
                for fut in concurrent.futures.as_completed(list(futures), timeout=10):
                    fut.result(timeout=.1)
                    futures.remove(done)
            except concurrent.futures.TimeoutError:
                pass

        writer_q.put({'write_state': (state,)})
        # Tell the writer thread to shut down.
        writer_q.put(None)
        # Wait for the writer to be complete.
        writer_fut.result()

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


@utils.handle_top_exception(LOGGER)
def main():
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
        client = HelpshiftAPI(args.config)
        do_sync(client, catalog, args.state, args.config)


if __name__ == "__main__":
    main()

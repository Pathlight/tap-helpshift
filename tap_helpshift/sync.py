import singer
from singer import metadata


LOGGER = singer.get_logger()


async def sync_stream(state, instance, counter, *args, start_date=None):
    stream = instance.stream
    mdata = stream.metadata

    stream_name = stream.tap_stream_id
    LOGGER.info("%s: Starting sync", stream_name)

    # If we have a bookmark, use it; otherwise use start_date & update bookmark with it
    if (instance.replication_method == 'INCREMENTAL' and
            not state.get('bookmarks', {}).get(stream.tap_stream_id, {}).get(instance.replication_key)):
        singer.write_bookmark(
            state,
            stream.tap_stream_id,
            instance.replication_key,
            start_date
        )

    # We keep our own count since the counter passed in may be shared by
    # other async fns.
    my_count = 0
    parent_stream = stream
    async for (stream, record) in instance.sync(state, *args):
        # NB: Only count parent records in the case of sub-streams
        if stream.tap_stream_id == parent_stream.tap_stream_id:
            counter.increment()
            my_count += 1

        with singer.Transformer() as transformer:
            rec = transformer.transform(record, stream.schema.to_dict(), metadata=metadata.to_map(mdata))
        singer.write_record(stream.tap_stream_id, rec)

    LOGGER.info("%s: Completed sync (%s rows)", stream_name, my_count)

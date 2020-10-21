#!/usr/bin/env python3
import os

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from .streams import CallsStream

REQUIRED_CONFIG_KEYS = ["start_date", "api_key"]

logger = singer.get_logger()
streams = [CallsStream.CallsStream()]

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def discover():
    entries = []

    for stream in streams:
        schema = Schema.from_dict(stream.get_schema())
        stream_metadata = []
        key_properties = stream.key_properties
        for prop, json_schema in schema.properties.items():
            inclusion = 'available'
            if prop in key_properties or prop == 'start_date':
                inclusion = 'automatic'

            stream_metadata.append({
                'breadcrumb': [],
                'metadata': {
                    'inclusion': 'available',
                    'table-key-properties': key_properties,
                    'schema-name': stream.tap_stream_id,
                    'selected': True,
                }
            })

            stream_metadata.append({
                'breadcrumb': ['properties', prop],
                'metadata': {
                    'inclusion': inclusion
                }
            })

        entries.append(
            CatalogEntry(
                tap_stream_id=stream.tap_stream_id,
                stream=stream.tap_stream_id,
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
    return Catalog(entries)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    selected_streams = catalog.get_selected_streams(state)

    selected_streams_ids = map(lambda stream: stream.tap_stream_id, selected_streams)

    logger.debug('Selected streams ids: {}'.format(str(selected_streams_ids)))

    for stream in streams:
        if stream.tap_stream_id not in selected_streams_ids:
            logger.debug('{} is not selected, skipping.'.format(stream.tap_stream_id))
            continue

        logger.info("Syncing stream:" + stream.tap_stream_id)

        stream.set_initial_bookmark(config, state)


        logger.info(catalog.get_stream(stream.tap_stream_id).schema)

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.get_schema(),
            key_properties=stream.key_properties,
            bookmark_properties=[stream.bookmark_field]
        )

        stream.sync(config, singer.metadata.to_map(catalog.get_stream(stream.tap_stream_id).metadata))

        if stream.bookmark_field:
            state = singer.write_bookmark(state, stream.tap_stream_id, stream.bookmark_field, str(stream.max_bookmark.add(seconds=1)))

        singer.write_state(state)


@utils.handle_top_exception(logger)
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
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()

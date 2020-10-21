import os

import time
import requests
import singer
import pendulum
from singer.schema import Schema
from requests.exceptions import ConnectionError, RequestException

logger = singer.get_logger()

API_ENDPOINT = 'https://public-api.ringover.com/v2'


class RingoverStream(object):
    tap_stream_id = ''
    key_properties = []
    schema = ''
    schema_path = 'schemas/{}.json'
    schema_cache = None

    bookmark_field = None
    initial_bookmark = None
    max_bookmark = None

    latest_response = None

    endpoint = ''
    limit = 1000

    def set_initial_bookmark(self, config, state):
        logger.info(self.tap_stream_id)
        logger.info(state)
        logger.info(singer.get_bookmark(state, self.tap_stream_id, self.bookmark_field))
        logger.info(state.get('bookmarks', {}).get(self.tap_stream_id, {}).get(self.bookmark_field))
        self.initial_bookmark = pendulum.parse(
            singer.get_bookmark(state, self.tap_stream_id, self.bookmark_field, config['start_date']))
        self.max_bookmark = self.initial_bookmark

    def get_schema(self):
        if not self.schema_cache:
            self.schema_cache = self.load_schema()
        return self.schema_cache

    def load_schema(self):
        schema_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..',
                                   self.schema_path.format(self.schema))
        schema = singer.utils.load_json(schema_path)
        return schema

    def execute_request(self, config, params=None):
        url = '{}/{}'.format(API_ENDPOINT, self.endpoint)
        _params = {
            'limit_count': self.limit
        }
        headers = {
            'User-Agent': config['user-agent'],
            'Authorization': config['api_key']
        }

        if params:
            _params.update(params)

        logger.info('Firing request at {} with params: {}'.format(url, params))

        return requests.get(url, headers=headers, params=_params)

    def sync(self, config, stream_metadata):
        has_more_data = True

        with singer.metrics.record_counter(self.endpoint) as counter:
            while has_more_data:
                with singer.metrics.http_request_timer(self.endpoint) as timer:
                    try:
                        response = self.execute_request(config, self.get_request_params())
                    except (ConnectionError, RequestException) as e:
                        raise e

                    timer.tags[singer.metrics.Tag.http_status_code] = response.status_code



                    if response.status_code == 429:
                        logger.debug('Hit the Ringover Rate limit, retrying...')
                        self.rate_throttle()
                        continue

                    if response.status_code == 204:
                        logger.info('Ringover API returned no content')
                        has_more_data = False
                        continue

                    if response.status_code == 400:
                        logger.info('Ringover API returned a bad request')
                        has_more_data = False
                        continue

                    self.latest_response = response

                    self.validate_response(response)
                    has_more_data = self.has_more_data(response)

                    logger.debug('Has more data')
                    self.rate_throttle()

                    with singer.Transformer(singer.NO_INTEGER_DATETIME_PARSING) as transformer:
                        for record in self.iterate_response(response):
                            record = self.process_record(record)
                            record = transformer.transform(record, self.get_schema(), stream_metadata)

                            if self.write_record(record):
                                counter.increment()

                            self.update_max_bookmark(record)

    def write_record(self, record):
        if self.record_is_newer_or_equal(record):
            singer.write_record(self.tap_stream_id, record, time_extracted=pendulum.now())
            return True
        return False

    def record_is_newer_or_equal(self, record):
        if self.bookmark_field is None or self.initial_bookmark is None:
            return True

        if self.get_record_bookmark(record) is None:
            return True

        current_bookmark = pendulum.parse(self.get_record_bookmark(record))
        if current_bookmark >= self.initial_bookmark:
            return True

        return False

    def bookmark_is_newer_or_equal(self, current_bookmark):
        if self.max_bookmark is None:
            return True

        if current_bookmark >= self.max_bookmark:
            return True

        return False

    def get_record_bookmark(self, record):
        return record[self.bookmark_field]

    def update_max_bookmark(self, record):
        if self.bookmark_field:
            if self.get_record_bookmark(record) is not None:
                current_bookmark = pendulum.parse(self.get_record_bookmark(record))

                if self.bookmark_is_newer_or_equal(current_bookmark):
                    self.max_bookmark = current_bookmark
        return True

    def validate_response(self, response):
        return True

    def transform_response(self, response):
        # Implement this function in child streams
        return response

    def process_record(self, record):
        # Implement this function in child streams
        return record

    def iterate_response(self, response):
        # Implement this function in child streams
        return []

    # Rate limit in ringover is 2requests per second, wait 500ms before the next request
    def rate_throttle(self):
        logger.debug('Waiting 300ms before firing another request')
        time.sleep(0.3)

    def has_more_data(self, response):
        return False

    def get_request_params(self):
        return None

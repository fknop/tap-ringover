from .stream import (RingoverStream)
import singer
import json

logger = singer.get_logger()


class CallsStream(RingoverStream):
    tap_stream_id = 'calls'
    endpoint = 'calls'
    schema = 'calls'
    key_properties = ['call_id']
    bookmark_field = 'start_time'

    all_ids = set()


    def get_request_params(self):
        last_record = None
        if self.latest_response is not None:
            payload = self.latest_response.json()
            last_record = None if payload['call_list'] is None else payload['call_list'][-1]


        return {
            'start_date': str(self.initial_bookmark),
            'last_id_returned': None if last_record is None else last_record.get('cdr_id')
        }

    def iterate_response(self, response):
        payload = response.json()
        return [] if payload['call_list'] is None else payload['call_list']

    def process_record(self, record):
        if record.get('cdr_id') in self.all_ids:
            logger.info('ID: {} is already fetched'.format(record.get('cdr_id')))
        self.all_ids.add(record.get('cdr_id'))
        ivr = record.get('ivr')
        record['ivr_id'] = ivr.get('ivr_id') if ivr is not None else None

        contact = record.get('contact')
        record['contact_id'] = contact.get('contact_id') if contact is not None else None

        user = record.get('user')
        record['user_id'] = user.get('user_id') if user is not None else None

        return record

    def has_more_data(self, response):


        payload = response.json()
        return payload['call_list_count'] >= self.limit

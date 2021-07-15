from tap_framework.streams import BaseStream as base
from tap_assembled.state import save_state

import singer
import singer.metrics
import time
import pytz

LOGGER = singer.get_logger()


class BaseStream(base):
    def get_url(self, path):
        return f"{self.client.base_url}{path}"

    def convert_utc_timestamp(self, dt):
        utc_time = dt.replace(tzinfo=pytz.utc)
        utc_timestamp = utc_time.timestamp()
        return utc_timestamp

    def convert_timestamp_utc(self, ts):
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))

    def sync(self):
        table = self.TABLE

        LOGGER.info(f"tap-assembled: syncing data for entity {table}")

        url = self.get_url(self.api_path)
        result = self.client.make_request(url, self.API_METHOD)

        data = self.get_stream_data(result)

        if len(data) > 0:
            with singer.metrics.record_counter(endpoint=table) as counter:
                for obj in data:
                    singer.write_records(table, [obj])

                counter.increment(len(data))

        save_state(self.state)

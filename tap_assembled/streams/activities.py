from tap_assembled.streams.base import BaseStream

import singer
import pytz

from datetime import timedelta, datetime, timezone
from tap_assembled.config import get_config_start_date
from tap_assembled.state import incorporate, save_state, get_last_record_value_for_table

LOGGER = singer.get_logger()


class ActivitiesStream(BaseStream):
    NAME = "ActivitiesStream"
    KEY_PROPERTIES = ["id", "agent_id"]
    API_METHOD = "GET"
    TABLE = "activities"

    @property
    def api_path(self):
        return "/activities"

    # activity sync over time period - incremental
    def sync_data(self):
        table = self.TABLE

        LOGGER.info(f"tap-assembled: syncing data for entity {table}")

        date = get_last_record_value_for_table(self.state, table)

        if not date:
            date = get_config_start_date(self.config)

        interval = timedelta(days=1)

        # sync incrementally - by day
        while date < datetime.now(pytz.utc):
            self.sync_for_period(date, interval)

            # keep bookmark updated
            self.state = incorporate(
                self.state, self.TABLE, "last_record", date.isoformat()
            )
            save_state(self.state)
            date = date + interval

    def get_stream_data(self, result):

        if not result or "activities" not in result:
            return []

        activities = []
        for activity in result.get("activities").values():

            # conversion from ts to utc, as singer does not support its transformation
            activity["start_time"] = self.convert_timestamp_utc(activity["start_time"])
            activity["end_time"] = self.convert_timestamp_utc(activity["end_time"])

            # data transformation by singer
            activities.append(self.transform_record(activity))

        return activities

    def sync_for_period(self, date, interval):
        table = self.TABLE

        date_from = round(self.convert_utc_timestamp(date))
        date_to = round(self.convert_utc_timestamp(date + interval))

        LOGGER.info(
            f"tap-assembled: syncing {table} table from {date.isoformat()} to {(date+interval).isoformat()}"
        )

        params = {"start_time": date_from, "end_time": date_to}
        url = f"{self.client.base_url}{self.api_path}"

        result = self.client.make_request(url, self.API_METHOD, params=params)
        data = self.get_stream_data(result)

        if len(data) > 0:
            with singer.metrics.record_counter(endpoint=table) as counter:
                singer.write_record(table, data)
                counter.increment(len(data))

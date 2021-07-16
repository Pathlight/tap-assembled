from tap_assembled.streams.base import BaseStream

import singer
import time
import datetime
import pytz
import tap_assembled.cache
from tap_assembled.config import get_config_start_date
from tap_assembled.state import incorporate, save_state

LOGGER = singer.get_logger()


class AgentStatesStream(BaseStream):
    NAME = "AgentStatesStream"
    KEY_PROPERTIES = ["agent_id", "agent_import_id", "platform", "start_time"]
    API_METHOD = "GET"
    TABLE = "agent_states"

    @property
    def api_path(self):
        return "/agents/{agent_id}/state"

    def sync_data(self):
        table = self.TABLE

        LOGGER.info(f"tap-assembled: syncing data for entity {table}")

        # sync its state per agent at this moment
        for agent in tap_assembled.cache.agents:
            agent_id = agent["id"]
            self.sync_for_agent(agent_id)

        self.state = incorporate(
            self.state,
            self.TABLE,
            "last_record",
            datetime.datetime.now(pytz.utc).isoformat(),
        )
        save_state(self.state)

    def get_stream_data(self, result, agent_id):
        if not result or "agent_states" not in result:
            return []

        xf = []
        for record in result["agent_states"]:

            # pre-filtering a record if the state is None - not useful. 
            if record["state"] is None: continue 

            # time conversion
            record["start_time"] = self.convert_timestamp_utc(record["start_time"])
            record_xf = self.transform_record(record)
            record_xf["agent_id"] = agent_id
            xf.append(record_xf)
        return xf

    def sync_for_agent(self, agent_id):
        table = self.TABLE

        url = f"{self.client.base_url}{self.api_path.format(agent_id=agent_id)}"
        result = self.client.make_request(url, self.API_METHOD)

        data = self.get_stream_data(result, agent_id)

        if len(data) > 0:
            with singer.metrics.record_counter(endpoint=table) as counter:
                for obj in data:
                    singer.write_records(table, [obj])
                counter.increment(len(data))

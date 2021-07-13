from tap_assembled.streams.base import BaseStream
import tap_assembled.cache
import singer

LOGGER = singer.get_logger()


class AgentsStream(BaseStream):
    NAME = "AgentsStream"
    KEY_PROPERTIES = ["id"]
    API_METHOD = "GET"
    TABLE = "agents"

    @property
    def api_path(self):
        return "/agents"

    def get_stream_data(self, result):
        if not result or "agents" not in result:
            return []

        agents = [self.transform_record(agent) for agent in result["agents"].values()]

        tap_assembled.cache.agents.extend(agents)
        return agents

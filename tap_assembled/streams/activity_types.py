from tap_assembled.streams.base import BaseStream
import singer

LOGGER = singer.get_logger()


class ActivityTypesStream(BaseStream):
    NAME = "ActivityTypesStream"
    KEY_PROPERTIES = ["id"]  # activity_type id
    API_METHOD = "GET"
    TABLE = "activity_types"

    @property
    def api_path(self):
        return "/activity_types"

    def get_stream_data(self, result):
        if not result or "activity_types" not in result:
            return []

        activity_types = [
            self.transform_record(activity)
            for activity in result["activity_types"].values()
        ]

        return activity_types

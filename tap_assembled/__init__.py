import tap_framework
from tap_framework.state import save_state

from tap_assembled.client import AssembledClient
from tap_assembled.streams import AVAILABLE_STREAMS

import singer
from singer import utils

LOGGER = singer.get_logger()


class AssembledRunner(tap_framework.Runner):
    def do_sync(self):
        LOGGER.info("tap-assembled: starting sync...")

        streams = self.get_streams_to_replicate()
        stream_map = {s.NAME: s for s in streams}

        for available_stream in AVAILABLE_STREAMS:
            if available_stream.NAME not in stream_map:
                continue

            stream = stream_map[available_stream.NAME]
            LOGGER.info(f" -> stream {available_stream.NAME}")

            try:
                stream.state = self.state
                stream.sync()
                self.state = stream.state
            except OSError as e:
                LOGGER.error(str(e))
                exit(e.errno)
            except Exception as e:
                LOGGER.error(str(e))
                LOGGER.error(f"Failed to sync endpoint {stream.TABLE}, moving on!")
                raise e

        save_state(self.state)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(required_config_keys=["api_key", "start_date"])

    client = AssembledClient(args.config)
    runner = AssembledRunner(args, client, AVAILABLE_STREAMS)

    if args.discover:
        runner.do_discover()
    else:
        runner.do_sync()


if __name__ == "__main__":
    main()

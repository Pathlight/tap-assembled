import requests
import singer
import backoff
import time

LOGGER = singer.get_logger()


class APIException(Exception):
    pass


class AssembledClient:
    MAX_TRIES = 7

    def __init__(self, config):
        self.config = config
        self.base_url = "https://api.assembledhq.com/v0"

    @backoff.on_exception(backoff.expo, APIException, max_tries=MAX_TRIES)
    def make_request(self, url, method, params=None, body=None):

        LOGGER.info("Making {} request to {} ({})".format(method, url, params))

        response = requests.request(
            method, url, auth=(self.config["api_key"], ""), params=params, json=body
        )

        if response.status_code == 429:
            LOGGER.info("Rate limit status code received.")
            time.sleep(1)  # a bit slow down
            raise APIException("Rate limit exceeded")

        elif response.status_code in [401, 403]:
            raise APIException("API Key expired")

        elif response.status_code == 404:
            LOGGER.info("Requested report not found (404).")
            return None

        elif response.status_code != 200:
            raise APIException(response.text)

        LOGGER.info(f"Got Status code {response.status_code}")
        return response.json()

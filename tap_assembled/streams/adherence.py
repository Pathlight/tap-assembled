from tap_assembled.streams.base import BaseStream

import singer
import pytz
import time

from datetime import timedelta, datetime
from tap_assembled.config import get_config_start_date
from tap_assembled.state import incorporate, save_state, get_last_record_value_for_table

LOGGER = singer.get_logger()


class AdherenceStream(BaseStream):
	NAME = "AdherenceStream"
	TABLE = "adherence"
	INTERVAL = "24h" # assumes a time range of > 1 day
	PHONE = "phone"
	EMAIL = "email"
	CHANNEL_TYPES = [PHONE, EMAIL]
	
	REPORT_GENERATION_PAUSE_IN_SECS = 10

	@property
	def api_path(self):
		return "/reports/adherence"

	def get_report(self, result):
		# retrieve the report id from the results and use it to retrieve the associated metrics
		if not result or "report_id" not in result:
			return []
		
		report_id = result.get("report_id")
		params = {"limit": 500} # default value; TODO: consider increasing or handle pagination
		url = f"{self.client.base_url}/reports/{report_id}"
		
		time.sleep(self.REPORT_GENERATION_PAUSE_IN_SECS)
		LOGGER.info(f"tap-assembled: retrieving report id {report_id}")
		result = self.client.make_request(url, "GET", params=params)
		status = result['status']

		while ("in_progress" == status): 
			# TODO:  eventually need to fail this process
			time.sleep(self.REPORT_GENERATION_PAUSE_IN_SECS)
			LOGGER.info(f"tap-assembled: re-trying report id {report_id}")
			result = self.client.make_request(url, "GET", params=params)
			status = result['status']
			
		#LOGGER.info("report contents:")
		#LOGGER.info(result['status'])
		LOGGER.info(result['total_metric_count'])
		return result
	
	def get_stream_data(self, result, channel):
		if not result or "metrics" not in result:
			return []

		metrics = []
		for metric in result["metrics"]:

			# flatten record
			metric["agent_id"] = metric["attributes"]["agent_id"]
			metric["type"] = metric["attributes"]["type"]
			
			# conversion from ts to utc, as singer does not support its transformation
			metric["start_time"] = self.convert_timestamp_utc(metric["attributes"]["start_time"])
			metric["end_time"] = self.convert_timestamp_utc(metric["attributes"]["end_time"])
			
			# add additional fields
			metric["channel"] = channel

			# data transformation by singer
			LOGGER.info(metric) # debug
			metrics.append(self.transform_record(metric))

		return metrics

	def sync_for_period_and_channel(self, date, interval, channel):
		table = self.TABLE

		date_from = round(self.convert_utc_timestamp(date))
		date_to = round(self.convert_utc_timestamp(date + interval))

		LOGGER.info(
			f"tap-assembled: syncing {table} table for {channel} from {date.isoformat()} to {(date+interval).isoformat()}"
		)
		
		body = {"start_time": date_from, "end_time": date_to, "interval": self.INTERVAL, "channel": channel}
		url = f"{self.client.base_url}{self.api_path}"

		result = self.client.make_request(url, "POST", body=body)
		report = self.get_report(result) 
		data = self.get_stream_data(report, channel)

		if len(data) > 0:
			with singer.metrics.record_counter(endpoint=table) as counter:
				for obj in data:
					singer.write_records(table, [obj])
				counter.increment(len(data))


	def sync_for_period(self, date, interval):
		for channel in self.CHANNEL_TYPES:
			self.sync_for_period_and_channel(date, interval, channel)
			
	# activity sync over time period - incremental
	def sync_data(self):
		table = self.TABLE

		LOGGER.info(f"tap-assembled: syncing data for entity {table}")

		date = get_last_record_value_for_table(self.state, table)

		if not date:
			date = get_config_start_date(self.config)

		interval_sliding = timedelta(days=1)
		interval = timedelta(days=7)

		# sync incrementally - by day
		while pytz.utc.localize(date) < datetime.now(pytz.utc):
			self.sync_for_period(date, interval)

			# keep bookmark updated
			self.state = incorporate(
				self.state, self.TABLE, "last_record", date.isoformat()
			)
			save_state(self.state)
			date = date + interval_sliding

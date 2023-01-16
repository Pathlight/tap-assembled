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
	KEY_PROPERTIES = []
	TABLE = "adherence"
	INTERVAL = "24h" # assumes a time range of > 1 day
	PHONE = "phone"
	EMAIL = "email"
	CHANNEL_TYPES = [PHONE, EMAIL]
	LIMIT = 500
	
	REPORT_GENERATION_PAUSE_IN_SECS = 10
	MAX_REPORT_ATTEMPTS = 6

	@property
	def api_path(self):
		return "/reports/adherence"

	def get_report_with_retries(self, result, report_id, params):		
		url = f"{self.client.base_url}/reports/{report_id}"
		
		LOGGER.info(f"tap-assembled: retrieving report id {report_id}")
		result = self.client.make_request(url, "GET", params=params)
		status = result['status']
		attempts_count = 0

		while ("in_progress" == status) & (attempts_count < self.MAX_REPORT_ATTEMPTS): 
			time.sleep(self.REPORT_GENERATION_PAUSE_IN_SECS)
			LOGGER.info(f"tap-assembled: re-trying report id {report_id}")
			result = self.client.make_request(url, "GET", params=params)
			status = result['status']
			attempts_count += 1
			
		if ("in_progress" == status):
			LOGGER.error(f"tap-assembled: timed out while retrieving report id {report_id}")
			
		return result
			
	def get_report(self, result, offset):
		# retrieve the report id from the results and use it to retrieve the associated metrics
		if not result or "report_id" not in result:
			return []
		
		report_id = result.get("report_id")
		
		params = {"limit": self.LIMIT, "offset":offset} 
		result = self.get_report_with_retries(result, report_id, params)	
		
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
			
			# and removed the unused key to avoid warnings
			del(metric["attributes"])

			# data transformation by singer
			metrics.append(self.transform_record(metric))

		return metrics
	
	def get_and_save_report_page(self, result, table, channel, offset):
		report = self.get_report(result, offset) 
		data = self.get_stream_data(report, channel)

		if len(data) > 0:
			with singer.metrics.record_counter(endpoint=table) as counter:
				for obj in data:
					singer.write_records(table, [obj])
				counter.increment(len(data))
		return report	

	def sync_for_period_and_channel(self, date, interval, channel):
		table = self.TABLE

		date_from = round(self.convert_utc_timestamp(date))
		date_to = round(self.convert_utc_timestamp(date + interval))

		LOGGER.info(f"tap-assembled: syncing {table} table for {channel} from {date.isoformat()} to {(date + interval).isoformat()}")
		
		body = {"start_time": date_from, "end_time": date_to, "interval": self.INTERVAL, "channel": channel}
		url = f"{self.client.base_url}{self.api_path}"

		result = self.client.make_request(url, "POST", body=body)		
		offset = 0
		report = self.get_and_save_report_page(result, table, channel, offset)
		
		if report:
			total = report['total_metric_count']
			count = len(report.get('metrics', []))
			LOGGER.debug(f"tap-assembled: processing {total} metrics")
			LOGGER.debug(f"tap-assembled: this batch had {count} metrics")	
			while (total > (offset + count)):	
				offset += count
				report = self.get_and_save_report_page(result, table, channel, offset)
				count = len(report['metrics'])
				LOGGER.debug(f"tap-assembled: processing {total} metrics")
				LOGGER.debug(f"tap-assembled: this batch had {count} metrics")	
			LOGGER.info(f"tap-assembled: processed {total} metrics")

	def sync_for_period(self, date, interval):
		for channel in self.CHANNEL_TYPES:
			self.sync_for_period_and_channel(date, interval, channel)
			
	# adherence sync over time period - incremental
	def sync_data(self):
		table = self.TABLE

		LOGGER.info(f"tap-assembled: syncing data for entity {table}")

		date = get_last_record_value_for_table(self.state, table)

		if not date:
			date = get_config_start_date(self.config)

		interval_sliding = timedelta(days=1)
		interval = timedelta(days=1)

		# Sync incrementally - by day, up through yesterday. Don't pull today's data yet because it is still being generated.
		date_format = '%Y-%m-%d %H:%M:%S.%z'
		end_window = datetime.now(pytz.utc) - interval
		LOGGER.info(f"tap-assembled: comparing {date.strftime(date_format)} to {end_window.strftime(date_format)}")

		while date < end_window:
			LOGGER.info(f"proceeding with sync for {date} and {interval}")
			self.sync_for_period(date, interval)

			# keep bookmark updated
			LOGGER.info(f"updating last_record to {(date + interval).isoformat()}")
			self.state = incorporate(
				self.state, self.TABLE, "last_record", (date + interval).isoformat()
			)
			save_state(self.state)
			date = date + interval_sliding

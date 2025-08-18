import logging

from datetime import datetime

from pandas import DataFrame


class BaseMonitor:
    def __init__(self, filename: str) -> None:
        self.log = logging.getLogger(filename)
        logging.basicConfig(filename=filename, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class StagingMonitor(BaseMonitor):
    def __init__(self, filename: str = "/var/log/staging_monitor.log") -> None:
        super().__init__(filename)

    def log_new_records(self, dataframe: DataFrame, new_records: DataFrame):
        self.log.info(f"[records]: {len(dataframe)}")
        self.log.info(f"[duplicated]: {len(dataframe) - len(new_records)}")
        self.log.info(f"[new records]: {len(new_records)}")


class PipelineMonitor(BaseMonitor):
    def __init__(self, filename: str = "/var/log/pipeline_monitor.log") -> None:
        super().__init__(filename)

    def log_extraction(self, source: str, batch_id: str, rows: int):
        self.log.info(f"[extract] source={source} batch_id={batch_id} rows={rows}")

    def log_transformations(self, source: str, batch_id: str, rows_in: int, rows_out: int):
        self.log.info(f"[transform] source={source} batch_id={batch_id} rows_in={rows_in} rows_out={rows_out}")

    def check_data_freshness(self, last_extracted_at: datetime):
        self.log.info(f"[freshness] last_extracted_at={last_extracted_at.isoformat()}")


class CronJobMonitor(BaseMonitor):
    def __init__(self, filename: str = "/var/log/cronjob_monitor.log") -> None:
        super().__init__(filename)
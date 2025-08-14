import logging

from datetime import datetime


class BaseMonitor:
    def __init__(self, filename: str) -> None:
        self.log = logging.getLogger(filename)
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class PipelineMonitor(BaseMonitor):
    def __init__(self, filename: str = "pipeline") -> None:
        super().__init__(filename)

    def log_extraction(self, source: str, batch_id: str, rows: int):
        self.log.info(f"[extract] source={source} batch_id={batch_id} rows={rows}")

    def log_transformations(self, source: str, batch_id: str, rows_in: int, rows_out: int):
        self.log.info(f"[transform] source={source} batch_id={batch_id} rows_in={rows_in} rows_out={rows_out}")

    def check_data_freshness(self, last_extracted_at: datetime):
        self.log.info(f"[freshness] last_extracted_at={last_extracted_at.isoformat()}")


class CronJobMonitor(BaseMonitor):
    def __init__(self, filename: str = "cronjob") -> None:
        super().__init__(filename)
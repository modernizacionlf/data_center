import logging

from datetime import datetime


log = logging.getLogger("pipeline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class PipelineMonitor:
    def log_extraction(self, source: str, batch_id: str, rows: int):
        log.info(f"[extract] source={source} batch_id={batch_id} rows={rows}")

    def log_transformations(self, source: str, batch_id: str, rows_in: int, rows_out: int):
        log.info(f"[transform] source={source} batch_id={batch_id} rows_in={rows_in} rows_out={rows_out}")

    def check_data_freshness(self, last_extracted_at: datetime):
        log.info(f"[freshness] last_extracted_at={last_extracted_at.isoformat()}")
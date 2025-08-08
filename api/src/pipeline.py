from typing import Any, Dict, Optional

import pandas as pd

from src.extractor import BaseExtractor
from src.staging import StagingLoader
from src.transform import DataTransformer
from src.monitoring import PipelineMonitor
from src.warehouse import WarehouseLoader


class DataPipeline:
    def __init__(self, target_connection_string: str, monitor: Optional[PipelineMonitor] = None) -> None:
        self.staging = StagingLoader(target_connection_string)
        self.warehouse = WarehouseLoader(target_connection_string)
        self.transformer = DataTransformer()
        self.monitor = monitor or PipelineMonitor()

    def run(
        self,
        extractor: BaseExtractor,
        extract_kwargs: Dict[str, Any],
        staging_table: Optional[str] = None,
        staging_schema: str = "staging",
        final_table: Optional[str] = None,
        final_schema: str = "warehouse",
    ) -> dict[str, Any]:
        raw: pd.DataFrame = extractor.extract(**extract_kwargs) # type: ignore
        self.monitor.log_extraction(extractor.source_name, extractor.batch_id, len(raw))

        stage_table = staging_table or f"{extractor.source_name}_raw"
        loaded_raw = self.staging.load_raw_data(raw, table_name=stage_table, schema=staging_schema)

        curated = self.transformer.transform_batch(raw)
        self.monitor.log_transformations(extractor.source_name, extractor.batch_id, len(raw), len(curated))

        warehouse_table = final_table or f"{extractor.source_name}"
        loaded_curated = self.warehouse.load_curated_data(curated, table_name=warehouse_table, schema=final_schema)

        return {
            "source": extractor.source_name,
            "batch_id": extractor.batch_id,
            "loaded_raw": loaded_raw,
            "loaded_curated": loaded_curated
        }

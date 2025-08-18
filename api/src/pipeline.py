from typing import Any, Dict, Optional, Union

import pandas as pd

from src.extractor import BaseExtractor
from src.staging import StagingLoader
from src.transform import DataTransformer
from src.monitoring import PipelineMonitor
from src.warehouse import WarehouseLoader
from utils.db_connections import QueryRequest


class LoadingStep:
    def __init__(self, loader: Union[StagingLoader, WarehouseLoader], schema: str) -> None:
        self.loader = loader
        self.schema = schema

    def execute(self, data: pd.DataFrame, table_name: str) -> int:
        return self.loader.load_data(data, table_name, self.schema, check_duplicates=True)


class TransformationStep:
    def __init__(self, transformer: DataTransformer) -> None:
        self.transformer = transformer

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        return self.transformer.transform_batch(data)
    

class PipelineContext:
    def __init__(self, source_name: str, batch_id: str) -> None:
        self.source_name = source_name
        self.batch_id = batch_id
        self.metadata: Dict[str, Any] = {}


class ExtractionStep:
    def __init__(self, extractor: BaseExtractor) -> None:
        self.extractor = extractor

    def execute(self, query_request: QueryRequest) -> tuple[pd.DataFrame, PipelineContext]:
        data = self.extractor.extract(query_request, add_metadata=True)
        context = PipelineContext(
            source_name=self.extractor.source_name,
            batch_id=self.extractor.batch_id
        )
        return data, context


class DataPipeline:
    def __init__(
        self,
        staging_loader: StagingLoader,
        warehouse_loader: WarehouseLoader,
        transformer: DataTransformer,
        monitor: Optional[PipelineMonitor] = None
    ):
        self.staging_step = LoadingStep(staging_loader, "staging")
        self.warehouse_step = LoadingStep(warehouse_loader, "warehouse")
        self.transform_step = TransformationStep(transformer)
        self.monitor = monitor or PipelineMonitor()

    def run(
        self,
        extractor: BaseExtractor,
        query_request: QueryRequest,
        staging_table: Optional[str] = None,
        final_table: Optional[str] = None,
    ) -> dict[str, Any]:
        extraction_step = ExtractionStep(extractor)
        raw_data, context = extraction_step.execute(query_request)
        
        self.monitor.log_extraction(context.source_name, context.batch_id, len(raw_data))

        staging_table = staging_table or f"{extractor.source_name}_raw"
        loaded_raw = self.staging_step.execute(raw_data, staging_table)

        curated_data = self.transform_step.execute(raw_data)
        self.monitor.log_transformations(context.source_name, context.batch_id, len(raw_data), len(curated_data))

        warehouse_table = final_table or f"{extractor.source_name}"
        loaded_curated = self.warehouse_step.execute(curated_data, warehouse_table)

        return {
            "source": context.source_name,
            "batch_id": context.batch_id,
            "loaded_raw": loaded_raw,
            "loaded_curated": loaded_curated
        }


from datetime import datetime
from pathlib import Path

import pandas as pd

from sqlalchemy import create_engine
from utils import QueryRequest



class BaseExtractor():
    def __init__(self, source_config: dict[str, str]) -> None:
        self.config = source_config

        self.source_name = source_config.get("name", "unknown_name")
        self.batch_id = f"{self.source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def extract(self, query_request: QueryRequest, add_metadata: bool = False) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement extract method")

    def _add_metadata(self, data: pd.DataFrame):
        data["_source"] = self.source_name
        data["_batch_id"] = self.batch_id
        data["_extracted_at"] = datetime.now()


class DatabaseExtractor(BaseExtractor):
    def __init__(self, source_config: dict[str, str]) -> None:
        super().__init__(source_config)
        self.connection_string = self.config.get("connection_string", "failed_connection_string")
    
    def extract(self, query_request: QueryRequest, add_metadata: bool = False) -> pd.DataFrame:
        engine = create_engine(self.connection_string)
        data: pd.DataFrame = pd.read_sql(  # type: ignore
            query_request.query, 
            engine, 
            params=query_request.params
        )
        if add_metadata:
            self._add_metadata(data)
        return data


class APIExtractor(BaseExtractor):
    pass


class FileExtractor(BaseExtractor):
    def __init__(self, source_config: dict[str, str]) -> None:
        super().__init__(source_config)
        self.base_path = Path(source_config.get("base_path", "files"))

    def extract(self, query_request: QueryRequest, add_metadata: bool = False) -> pd.DataFrame:
        filename = query_request.query
        file_path = self.base_path / filename
        data: pd.DataFrame = pd.read_json(file_path)
        if add_metadata:
            self._add_metadata(data)
        return data

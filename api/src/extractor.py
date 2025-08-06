from datetime import datetime

from sqlalchemy import create_engine

import pandas as pd



class BaseExtractor():
    def __init__(self, source_config: dict[str, str]) -> None:
        self.config = source_config

        self.source_name = source_config.get("name", "unknown_name")
        self.batch_id = f"{self.source_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}"

    def extract(self, query: str):
        pass

    def _add_metadata(self, data: pd.DataFrame):
        data["_source"] = self.source_name
        data["_batch_id"] = self.batch_id
        data["_extracted_at"] = datetime.now()


class DatabaseExtractor(BaseExtractor):
    def __init__(self, source_config: dict[str, str]) -> None:
        self.connection_string = self.config.get("connection_string", "failed_connection_string")
        super().__init__(source_config)

    def extract(self, query: str):
        engine = create_engine(self.connection_string)
        data: pd.DataFrame = pd.read_sql(query, engine) # type: ignore
        return self._add_metadata(data)


class APIExtractor(BaseExtractor):
    pass


class FileExtractor(BaseExtractor):
    pass
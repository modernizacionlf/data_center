from typing import Any

from src.extractor import DatabaseExtractor, QueryRequest
from utils.db_connections import DataCenter
from utils.paths import DATA_CENTER_PRODUCTION_PATH


ALLOWED_ORIGINS = [
    "http://data.lasflores.net.ar:8002",
    "https://data.lasflores.net.ar:8002"
]

ALLOWED_HOSTS = ["data.lasflores.gob.ar", "data.lasflores.net.ar", "localhost", "127.0.0.1"]

class ENDPOINTS:
    BASE = "/api"
    HEALTH = f"{BASE}/health"
    ENTITIES = f"{BASE}/entidades"

    @classmethod
    def ENTITY(cls, entity_name: str):
        return f"{cls.ENTITIES}/{entity_name}"

    @classmethod
    def STATISTICS(cls, entity_name: str):
        return f"{cls.ENTITIES}/{entity_name}/estadisticas"


datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)
extractor = DatabaseExtractor(datacenter.source_config)

def get_available_entities() -> list[str]:
    query_request = QueryRequest(
        query="""
            SELECT table_name
            FROM information_schema.tables
	        WHERE table_schema = 'warehouse'
	        AND table_type = 'BASE TABLE'
	        ORDER BY table_name
        """
    )
    enitites_dataframe = extractor.extract(query_request)
    entities = enitites_dataframe["table_name"].to_list()
    return entities

def get_available_statistics(entity_name: str) -> dict[str, list[dict[str, Any]]]:
    stats: dict[str, list[dict[str, Any]]] = {}
    excluded_columns = ["_source", "_batch_id", "_extracted_at", "record_hash"]
    query_request = QueryRequest(
        query=f'SELECT * from warehouse."{entity_name}"',
        params={"table": entity_name}
    )
    entity_dataframe = extractor.extract(query_request)
    entity_dataframe = entity_dataframe.drop(columns=excluded_columns, errors="ignore")
    available_columns = entity_dataframe.columns.tolist()

    for column in available_columns:
        if entity_dataframe[column].dtype in ["object", "bool"] or (entity_dataframe[column].dtype in ["int64"] and column not in ["id"]):
            grouped_df = entity_dataframe.groupby(column).size().reset_index(name="cantidad") # type: ignore
            grouped_df = grouped_df.rename(columns={column: "valor"})
            stats[column] = grouped_df.to_dict('records') # type: ignore

    return stats
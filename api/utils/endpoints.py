from typing import Any

from .db_connections import DataCenter, QueryRequest
from .paths import DATA_CENTER_PRODUCTION_PATH


ALLOWED_ORIGINS = [
    "http://data.lasflores.net.ar:8002",
    "https://data.lasflores.net.ar:8002"
]

ALLOWED_HOSTS = ["data.lasflores.gob.ar", "data.lasflores.net.ar", "localhost", "127.0.0.1"]

RAW_DATA_ENTITIES = [
    "surtidores"
]

METADATA_COLUMNS = ["_source", "_batch_id", "_extracted_at", "record_hash"]

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



def get_available_entities() -> list[str]:
    from src import DatabaseExtractor

    datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)
    extractor = DatabaseExtractor(datacenter.source_config)

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
    from src import DatabaseExtractor

    datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)
    extractor = DatabaseExtractor(datacenter.source_config)

    query_request = QueryRequest(
        query=f'SELECT * from warehouse."{entity_name}"',
        params={"table": entity_name}
    )
    entity_dataframe = extractor.extract(query_request)
    entity_dataframe = entity_dataframe.drop(columns=METADATA_COLUMNS, errors="ignore")

    if entity_name in RAW_DATA_ENTITIES:
        return {
            "data": entity_dataframe.to_dict('records')
        }

    stats: dict[str, list[dict[str, Any]]] = {}
    available_columns = entity_dataframe.columns.tolist()

    for column in available_columns:
        if entity_dataframe[column].dtype in ["object", "bool"] or (entity_dataframe[column].dtype in ["int64"] and column not in ["id"]):
            grouped_df = entity_dataframe.groupby(column).size().reset_index(name="cantidad") # type: ignore
            grouped_df = grouped_df.rename(columns={column: "valor"})
            stats[column] = grouped_df.to_dict('records') # type: ignore

    return stats

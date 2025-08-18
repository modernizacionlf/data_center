from src.extractor import DatabaseExtractor, QueryRequest
from utils.db_connections import DataCenter
from utils.paths import DATA_CENTER_PRODUCTION_PATH


class ENDPOINTS:
    BASE = "/api"
    ENTITIES = f"{BASE}/entidades"
    
    @classmethod
    def ENTITY(cls, entity_name: str):
        return f"{cls.ENTITIES}/{entity_name}"


def get_available_entities() -> list[str]:
    datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)
    query_request = QueryRequest(
        query="""
            SELECT table_name
            FROM information_schema.tables
	        WHERE table_schema = 'warehouse'
	        AND table_type = 'BASE TABLE'
	        ORDER BY table_name
        """
    )
    extractor = DatabaseExtractor(datacenter.source_config)
    enitites_dataframe = extractor.extract(query_request)
    entities = enitites_dataframe["table_name"].to_list()
    return entities
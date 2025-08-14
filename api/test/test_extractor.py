from unittest import TestCase

from utils.db_connections import Geonode, QueryRequest
from utils.paths import GEONODE_ENV_PATH
from src.extractor import DatabaseExtractor


class TestDatabaseExtractor(TestCase):
    def setUp(self) -> None:
        geonode = Geonode(GEONODE_ENV_PATH)
        self.database_extractor = DatabaseExtractor(geonode.source_config)

    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_database_connection(self):
        query_request = QueryRequest(query="SELECT * FROM global.rampas LIMIT 1")
        data = self.database_extractor.extract(query_request)

        self.assertEqual(len(data), 1)
from unittest import TestCase

from utils.db_connections import Geonode
from utils.paths import GEONODE_ENV_PATH
from src.extractor import DatabaseExtractor


class TestDatabaseExtractor(TestCase):
    def setUp(self) -> None:
        geonode = Geonode(GEONODE_ENV_PATH)

        source_config = {
            "name": "geonode",
            "connection_string": geonode.connection_string
        }
        self.database_extractor = DatabaseExtractor(source_config)

    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_database_connection(self):
        query = "SELECT * FROM global.rampas LIMIT 1"
        params = None
        data = self.database_extractor.extract(query, params)

        self.assertEqual(len(data), 1)
import os

from dotenv import load_dotenv
from unittest import TestCase

from src.extractor import DatabaseExtractor
from src.paths import GEONODE_ENV_PATH


class TestDatabaseExtractor(TestCase):
    def setUp(self) -> None:
        load_dotenv(GEONODE_ENV_PATH)
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")

        source_config = {
            "name": "geonode",
            "connection_string": f"postgresql://geonode:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/geonode"
        }
        self.database_extractor = DatabaseExtractor(source_config)

    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_database_connection(self):
        query = "SELECT * FROM global.rampas LIMIT 1"
        params = None
        data = self.database_extractor.extract(query, params)

        self.assertEqual(len(data), 1)
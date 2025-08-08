import os

from dotenv import load_dotenv
from unittest import TestCase

from src.extractor import DatabaseExtractor
from src.paths import GEONODE_ENV_PATH


class TestDatabaseExtractor(TestCase):
    def setUp(self) -> None:
        load_dotenv(GEONODE_ENV_PATH)
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")

        source_config = {
            "name": "geonode",
            "connection_string": f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        }
        self.database_extractor = DatabaseExtractor(source_config)

    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_database_connection(self):
        query = "SELECT * FROM global.rampas LIMIT 1"
        params = None
        data = self.database_extractor.extract(query, params)

        self.assertEqual(len(data), 1)
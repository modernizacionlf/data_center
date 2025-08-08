import os 

from dotenv import load_dotenv
from unittest import TestCase

from src.extractor import DatabaseExtractor
from src.paths import DATA_CENTER_ENV_PATH, GEONODE_ENV_PATH
from src.pipeline import DataPipeline


class TestDataPipeline(TestCase):
    def setUp(self) -> None:
        return super().setUp()
    
    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_data_pipeline(self):
        load_dotenv(DATA_CENTER_ENV_PATH)
        POSTGRES_USER = os.getenv("POSTGRES_USER")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
        POSTGRES_DB = os.getenv("POSTGRES_DB")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST")
        TARGET_DB_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

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
        extractor = DatabaseExtractor(source_config)

        pipeline = DataPipeline(target_connection_string=TARGET_DB_URL)
        result = pipeline.run(
            extractor,
            extract_kwargs={"query": "SELECT * FROM global.rampas LIMIT 5"},
            staging_schema="staging",
            final_schema="warehouse"
        )
        loaded_raw = result["loaded_raw"]
        self.assertEqual(loaded_raw, 5)

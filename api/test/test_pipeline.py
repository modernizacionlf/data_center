from unittest import TestCase

from utils.db_connections import DataCenter, Geonode
from utils.paths import DATA_CENTER_ENVIRONMENT_PATH, GEONODE_ENV_PATH
from src.extractor import DatabaseExtractor
from src.pipeline import DataPipeline


class TestDataPipeline(TestCase):
    def setUp(self) -> None:
        return super().setUp()
    
    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_data_pipeline(self):
        geonode = Geonode(GEONODE_ENV_PATH)
        datacenter = DataCenter(DATA_CENTER_ENVIRONMENT_PATH)

        source_config = {
            "name": "geonode",
            "connection_string": geonode.connection_string
        }
        extractor = DatabaseExtractor(source_config)

        pipeline = DataPipeline(target_connection_string=datacenter.connection_string)
        result = pipeline.run(
            extractor,
            extract_kwargs={"query": "SELECT * FROM global.rampas LIMIT 5"},
            staging_schema="staging",
            final_schema="warehouse"
        )
        loaded_raw = result["loaded_raw"]
        self.assertEqual(loaded_raw, 5)

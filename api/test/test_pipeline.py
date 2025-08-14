from unittest import TestCase

from src.extractor import DatabaseExtractor
from src.pipeline import DataPipeline
from src.staging import StagingLoader
from src.transform import DataTransformer
from src.warehouse import WarehouseLoader
from utils.db_connections import DataCenter, Geonode
from utils.paths import DATA_CENTER_ENVIRONMENT_PATH, GEONODE_ENV_PATH


class TestDataPipeline(TestCase):
    def setUp(self) -> None:
        return super().setUp()
    
    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_data_pipeline(self):
        geonode = Geonode(GEONODE_ENV_PATH)
        datacenter = DataCenter(DATA_CENTER_ENVIRONMENT_PATH)
        
        staging_loader = StagingLoader(datacenter.connection_string)
        warehouse_loader = WarehouseLoader(datacenter.connection_string)
        transformer = DataTransformer()

        pipeline = DataPipeline(
            staging_loader,
            warehouse_loader,
            transformer
        )

        extractor = DatabaseExtractor(geonode.source_config)
        result = pipeline.run(
            extractor,
            extract_kwargs={"query": "SELECT * FROM global.rampas LIMIT 5"},
        )
        loaded_raw = result["loaded_raw"]
        self.assertEqual(loaded_raw, 5)

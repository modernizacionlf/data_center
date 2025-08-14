from unittest import TestCase

from api.src.extractor import DatabaseExtractor
from api.src.pipeline import DataPipeline
from api.src.staging import StagingLoader
from api.src.transform import DataTransformer
from api.src.warehouse import WarehouseLoader
from api.utils.db_connections import DataCenter, Geonode, QueryRequest
from api.utils.paths import DATA_CENTER_PRODUCTION_PATH, GEONODE_ENV_PATH


class TestDataPipeline(TestCase):
    def setUp(self) -> None:
        return super().setUp()
    
    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_data_pipeline(self):
        geonode = Geonode(GEONODE_ENV_PATH)
        datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)
        
        staging_loader = StagingLoader(datacenter.connection_string)
        warehouse_loader = WarehouseLoader(datacenter.connection_string)
        transformer = DataTransformer()

        pipeline = DataPipeline(
            staging_loader,
            warehouse_loader,
            transformer
        )

        extractor = DatabaseExtractor(geonode.source_config)
        query_request = QueryRequest(query="SELECT * FROM global.rampas LIMIT 5")
        result = pipeline.run(
            extractor,
            query_request,
        )
        loaded_raw = result["loaded_raw"]
        self.assertEqual(loaded_raw, 5)

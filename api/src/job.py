from typing import Sequence

from src.extractor import DatabaseExtractor
from src.monitoring import CronJobMonitor
from src.pipeline import DataPipeline
from src.staging import StagingLoader
from src.transform import DataTransformer
from src.warehouse import WarehouseLoader
from utils.db_connections import DBConnection, DataCenter, Geonode, BaseUnica
from utils.paths import DATA_CENTER_PRODUCTION_PATH, GEONODE_ENV_PATH

datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)


class DatabaseJob():
    def __init__(self, dbconnections: Sequence[DBConnection]) -> None:
        self.dbconnections = dbconnections

    def run(self):
        for database in self.dbconnections:
            staging_loader = StagingLoader(datacenter.connection_string)
            warehouse_loader = WarehouseLoader(datacenter.connection_string)
            transformer = DataTransformer()
            
            pipeline = DataPipeline(
                staging_loader,
                warehouse_loader,
                transformer
            )

            extractor = DatabaseExtractor(database.source_config)
            for query_request in database.queries:
                pipeline.run(
                    extractor,
                    query_request
                )


if __name__ == "__main__":
    monitor = CronJobMonitor()
    monitor.log.info("Inicio de ejecucion del job")
    try:
        geonode = Geonode(GEONODE_ENV_PATH)
        base_unica = BaseUnica(BASE_UNICA_ENV_PATH)
        dbconnections = [geonode, base_unica]
        DatabaseJob(dbconnections).run()
        monitor.log.info("Ejecución finalizada correctamente")
    except Exception as error:
        monitor.log.error(f"Error en la ejecución: {error}")

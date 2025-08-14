from typing import Sequence

from api.src.extractor import DatabaseExtractor
from api.src.monitoring import CronJobMonitor
from api.src.pipeline import DataPipeline
from api.src.staging import StagingLoader
from api.src.transform import DataTransformer
from api.src.warehouse import WarehouseLoader
from api.utils.db_connections import DBConnection, DataCenter, Geonode
from api.utils.paths import DATA_CENTER_PRODUCTION_PATH, GEONODE_ENV_PATH

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
                    query_request,
                )


if __name__ == "__main__":
    monitor = CronJobMonitor()
    monitor.log.info("Inicio de ejecucion del job")
    try:
        geonode = Geonode(GEONODE_ENV_PATH)
        dbconnections = [geonode]
        DatabaseJob(dbconnections).run()
        monitor.log.info("Ejecución finalizada correctamente")
    except Exception as error:
        monitor.log.error(f"Error en la ejecución: {error}")
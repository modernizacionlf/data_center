from typing import Sequence

from src.extractor import DatabaseExtractor
from src.monitoring import CronJobMonitor
from src.pipeline import DataPipeline
from src.staging import StagingLoader
from src.transform import DataTransformer
from src.warehouse import WarehouseLoader
from utils.db_connections import DBConnection, DataCenter, Geonode, QueryRequest
from utils.paths import DATA_CENTER_PRODUCTION_PATH, GEONODE_ENV_PATH

datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)


class DatabaseJob():
    def __init__(self, dbconnections: Sequence[DBConnection]) -> None:
        self.dbconnections = dbconnections

    def _extract_query_table_name(self, query_request: QueryRequest) -> str:
        return query_request.query.split('.')[1]

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
                table_name = self._extract_query_table_name(query_request)
                pipeline.run(
                    extractor,
                    query_request,
                    staging_table=table_name,
                    final_table=table_name
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
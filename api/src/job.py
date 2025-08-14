from typing import Sequence

from src.extractor import DatabaseExtractor
from src.monitoring import CronJobMonitor
from src.pipeline import DataPipeline
from utils.db_connections import DBConnection, DataCenter, Geonode
from utils.paths import DATA_CENTER_PRODUCTION_PATH, GEONODE_ENV_PATH

datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)


class DatabaseJob():
    def __init__(self, dbconnections: Sequence[DBConnection]) -> None:
        self.dbconnections = dbconnections

    def run(self):
        for database in self.dbconnections:
            extractor = DatabaseExtractor(database.source_config)
            pipeline = DataPipeline(target_connection_string=datacenter.connection_string)

            for query in database.queries:
                pipeline.run(
                    extractor,
                    extract_kwargs=query,
                    staging_schema="staging",
                    final_schema="warehouse"
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
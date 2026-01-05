from typing import Sequence

from .extractor import DatabaseExtractor, FileExtractor
from .monitoring import CronJobMonitor
from .pipeline import DataPipeline
from .staging import StagingLoader
from .transform import DataTransformer
from .warehouse import WarehouseLoader
from utils import DBConnection, DataCenter, Geonode, BaseUnica, QueryRequest
from utils import DATA_CENTER_PRODUCTION_PATH, FILES_PATH

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

class FileJob():
    def __init__(self, filenames: Sequence[str], base_path: str = "files") -> None:
        self.filenames = filenames
        self.base_path = base_path

    def run(self):
        for filename in self.filenames:
            staging_loader = StagingLoader(datacenter.connection_string)
            warehouse_loader = WarehouseLoader(datacenter.connection_string)
            transformer = DataTransformer()

            pipeline = DataPipeline(
                staging_loader,
                warehouse_loader,
                transformer
            )

            file_config = {
                "name": f"file_{filename.replace('.json', '')}",
                "base_path": self.base_path
            }
            extractor = FileExtractor(file_config)

            query_request = QueryRequest(
                query=filename,
                main_table=filename.replace('.json', ''),
                params=None
            )

            pipeline.run(
                extractor,
                query_request
            )


if __name__ == "__main__":
    monitor = CronJobMonitor()
    monitor.log.info("Inicio de ejecucion del job")
    try:
        geonode = Geonode()
        base_unica = BaseUnica()
        
        dbconnections: Sequence[DBConnection] = [geonode, base_unica]
        DatabaseJob(dbconnections).run()

        file_list = ["surtidores.json"]
        FileJob(file_list, base_path=FILES_PATH).run()

        monitor.log.info("Ejecución finalizada correctamente")
    except Exception as error:
        monitor.log.error(f"Error en la ejecución: {error}")

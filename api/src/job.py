from src.extractor import DatabaseExtractor
from src.pipeline import DataPipeline
from utils.db_connections import DataCenter, Geonode
from utils.paths import DATA_CENTER_PRODUCTION_PATH, GEONODE_ENV_PATH

from crontab import CronTab

LOG_PATH = "/var/log/data_center_job.log"

cron = CronTab(user="root")
job = cron.new(
    command=f"/usr/bin/python3 /src/job.py >> {LOG_PATH} 2>&1", 
    comment="Data Center Job"
)
job.setall("0 6 * * *")
cron.write()

datacenter = DataCenter(DATA_CENTER_PRODUCTION_PATH)


class DatabaseJob():
    def __init__(self) -> None:
        self.geonode = Geonode(GEONODE_ENV_PATH)
        self.dbconnections = [self.geonode]

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
    import logging
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info("Inicio de ejecucion del job")
    try:
        DatabaseJob().run()
        logging.info("Ejecución finalizada correctamente")
    except Exception as error:
        logging.error(f"Error en la ejecución: {error}")
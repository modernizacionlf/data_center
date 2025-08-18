import hashlib

from typing import Set, List, Dict, Any, Optional

from pandas import DataFrame
from sqlalchemy import create_engine, text, inspect

from src.monitoring import LoaderMonitor


class BaseLoader:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

        self.monitor = LoaderMonitor()

    def ensure_record_hash_column(self, table_name: str, schema: str) -> None:
        try:
            inspector = inspect(self.engine)
            
            if not inspector.has_table(table_name, schema=schema):
                self.monitor.log.info(f"Tabla {schema}.{table_name} no existe aún, se creará automáticamente")
                return
            
            columns = inspector.get_columns(table_name, schema=schema)
            column_names = [column['name'] for column in columns]
            
            if 'record_hash' not in column_names:
                self.monitor.log.info(f"Añadiendo columna record_hash a {schema}.{table_name}")
                with self.engine.connect() as connection:
                    alter_query = text(f"""
                        ALTER TABLE {schema}.{table_name} 
                        ADD COLUMN record_hash VARCHAR(32)
                    """)
                    connection.execute(alter_query)
                    
                    index_query = text(f"""
                        CREATE INDEX IF NOT EXISTS idx_{table_name}_record_hash 
                        ON {schema}.{table_name}(record_hash)
                    """)
                    connection.execute(index_query)
                    connection.commit()
                    self.monitor.log.info(f"Columna record_hash e índice creados en {schema}.{table_name}")
            else:
                self.monitor.log.info(f"Columna record_hash ya existe en {schema}.{table_name}")     
        except Exception as error:
            self.monitor.log.info(f"Error al verificar/crear columna record_hash: {error}")

    def get_existing_record_hashes(self, table_name: str, schema: str) -> Set[str]:
        try:
            with self.engine.connect() as connection:
                query = text(f"SELECT record_hash FROM {schema}.{table_name} WHERE record_hash IS NOT NULL")
                result = connection.execute(query)
                return {row[0] for row in result.fetchall()}
        except Exception:
            return set()

    def calculate_record_hash(self, record: Dict[str, Any], exclude_fields: Optional[List[str]] = None) -> str:
        if exclude_fields is None:
            exclude_fields = ['created_at', 'updated_at', '_batch_id', '_extracted_at', 'record_hash']
        
        stable_fields = {key: value for key, value in record.items() if key not in exclude_fields}
        record_str = str(sorted(stable_fields.items()))
        
        return hashlib.md5(record_str.encode()).hexdigest()
    
    def drop_duplicates(self, dataframe: DataFrame) -> DataFrame:
        dataframe_copy = dataframe.drop_duplicates(subset=['record_hash'])
        return dataframe_copy

    def add_record_hashes(self, dataframe: DataFrame, exclude_fields: Optional[List[str]] = None) -> DataFrame:
        dataframe_copy = dataframe.copy()
        dataframe_copy['record_hash'] = dataframe_copy.apply(
            lambda row: self.calculate_record_hash(row.to_dict(), exclude_fields), axis=1 # type: ignore
        )

        dataframe_copy = self.drop_duplicates(dataframe_copy)
        return dataframe_copy

    def filter_new_records(self, dataframe: DataFrame, table_name: str, schema: str) -> DataFrame:
        if dataframe.empty:
            return dataframe

        existing_hashes = self.get_existing_record_hashes(table_name, schema)
        
        new_records_mask = ~dataframe['record_hash'].isin(existing_hashes) # type: ignore
        new_records = dataframe[new_records_mask]

        self.monitor.log_new_records(dataframe, new_records)
        
        return new_records

    def load_data(self, dataframe: DataFrame, table_name: str, schema: str, check_duplicates: bool = True) -> int:
        if check_duplicates:
            self.ensure_record_hash_column(table_name, schema)

        if 'record_hash' not in dataframe.columns:
            dataframe = self.add_record_hashes(dataframe)
        else:
            dataframe = self.drop_duplicates(dataframe)

        if check_duplicates:
            dataframe = self.filter_new_records(dataframe, table_name, schema)
        else:
            dataframe = dataframe

        if dataframe.empty:
            return 0
        else:
            dataframe.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1_000,
            )
            return len(dataframe)
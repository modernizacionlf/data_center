from pandas import DataFrame
from src.loader import BaseLoader


class WarehouseLoader(BaseLoader):
    def __init__(self, connection_string: str):
        super().__init__(connection_string)

    def load_data(self, dataframe: DataFrame, table_name: str, schema: str = "warehouse") -> int:
        return super().load_data(dataframe, table_name, schema)

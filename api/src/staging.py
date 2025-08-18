from pandas import DataFrame

from src.loader import BaseLoader


class StagingLoader(BaseLoader):
    def __init__(self, connection_string: str) -> None:
        super().__init__(connection_string)

    def load_data(
        self, 
        dataframe: DataFrame, 
        table_name: str, 
        schema: str = "staging",
        check_duplicates: bool = True
    ) -> int:
        return super().load_data(dataframe, table_name, schema, check_duplicates)
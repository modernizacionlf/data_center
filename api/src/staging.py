import pandas as pd

from sqlalchemy import create_engine


class StagingLoader:
    def __init__(self, target_connection_string: str) -> None:
        self.engine = create_engine(target_connection_string)

    def load_raw_data(self, dataframe: pd.DataFrame, table_name: str, schema: str = "staging") -> int:
        if dataframe.empty:
            return 0
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
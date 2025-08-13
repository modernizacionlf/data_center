from sqlalchemy import create_engine

import pandas as pd


class BaseLoader:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def load_data(self, dataframe: pd.DataFrame, table_name: str, schema: str) -> int:
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
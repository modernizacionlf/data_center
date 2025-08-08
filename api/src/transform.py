import pandas as pd


class DataTransformer:
    def __init__(self) -> None:
        pass

    def transform_batch(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe = self._apply_transformations(dataframe.copy())
        self._validate_data(dataframe)
        return dataframe

    def _apply_transformations(self, dataframe: pd.DataFrame):
        dataframe.columns = [column.strip().lower().replace(" ", "_") for column in dataframe.columns]
        for column in dataframe.select_dtypes(include="object"):
            dataframe[column] = dataframe[column].astype(str).str.strip()
        return dataframe

    def _validate_data(self, dataframe: pd.DataFrame):
        if "_source" not in dataframe.columns or "_batch_id" not in dataframe.columns:
            raise ValueError("Missing metadata in the DataFrame")
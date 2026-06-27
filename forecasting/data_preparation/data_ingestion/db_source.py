import pandas as pd
from forecasting.utils.database import read_sql
from forecasting.data_preparation.data_ingestion.base import BaseSource

QUERY = """
SELECT *
FROM mart.mart_fresh_food_order_sale
WHERE date >= '2025-11-01'
"""

class DBSource(BaseSource):

    def fetch(self) -> pd.DataFrame:
        return read_sql(QUERY)
import sqlalchemy
import pandas as pd
from typing import Optional

def get_engine(sqlite_path: str = "data/crypto_data.sqlite") -> sqlalchemy.engine.Engine:
    engine = sqlalchemy.create_engine(f"sqlite:///{sqlite_path}", future=True)
    return engine

def write_df_to_sql(df: pd.DataFrame, table_name: str = "crypto_prices", engine: Optional[sqlalchemy.engine.Engine] = None):
    if engine is None:
        engine = get_engine()
    # Deduplicate BEFORE writing in production. Here we append and rely on run_pipeline dedupe.
    df.to_sql(table_name, engine, if_exists="append", index=False)

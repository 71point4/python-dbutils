import pandas as pd
import polars as pl
from ._utils import _connect
import logging

log = logging.getLogger(__name__)

def sql_query(self, sql):
    """
    Connect to a database and run a SQL query
    """
    try: 

        engine = _connect(self)
        log.debug(f'{sql}')

        with engine.begin() as conn:
            df = pd.read_sql_query(sql, con = conn)
            for col in df.columns:
                df[col] = df[col].apply(lambda x: x.decode("utf-8", errors="ignore") if isinstance(x, bytes) else x)
            df = pl.from_pandas(df)

        return df

    except Exception:
        raise
    finally:
        engine.dispose()
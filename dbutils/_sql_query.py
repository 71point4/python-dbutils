import pandas as pd
import polars as pl
from ._utils import _connect
import logging
import pyarrow as pa

log = logging.getLogger(__name__)


def sql_query(self, sql):
    """
    Execute a SQL query and return the results as a Polars DataFrame.

    This method establishes a database connection, runs the provided SQL
    statement, and loads the results into a pandas DataFrame before converting
    it to a Polars DataFrame.

    Parameters
    ----------
    sql : str
        SQL query to execute.

    Returns
    -------
    polars.DataFrame
        Query results returned as a Polars DataFrame.

    Raises
    ------
    Exception
        Propagates any exception raised during query execution.

    Notes
    -----
    - Query results are first read using ``pandas.read_sql_query``.
    - Columns containing byte strings are decoded to UTF-8.
    - The resulting pandas DataFrame is converted to a Polars DataFrame.
    """

    engine = None

    try:
        engine = _connect(self)

        log.debug(sql)

        with engine.begin() as conn:
            result = conn.exec_driver_sql(sql)

            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
            else:
                return None

        # decode byte columns if present
        for col in df.select_dtypes(include=["object"]):
            df[col] = df[col].apply(
                lambda x: x.decode("utf-8", errors="ignore") if isinstance(x, bytes) else x
            )

        table = pa.Table.from_pandas(df)
        result = pl.from_arrow(table)

        log.debug(f"Query returned {result.height} rows")

        return result

    except Exception:
        raise

    finally:
        if engine is not None:
            engine.dispose()
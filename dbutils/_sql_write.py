import pandas as pd
from ._utils import _connect
import logging
from math import ceil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from gc import collect

log = logging.getLogger(__name__)

def _clickhouse_write(self, df, table_name):

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=self.db_host,
        port=18123,
        username=self.db_user,
        password=self.db_pass,
        database=self.db,
    )

    client.insert_df(table_name, df)

def _sql_write(self, df, schema, table_name):
    """
    Write a pandas DataFrame to a database table.

    This internal helper establishes a database connection and appends the
    provided DataFrame to the specified table using ``pandas.to_sql``.
    Data is written in batches to improve performance for large datasets.

    Parameters
    ----------
    self : Query
        Instance of the ``Query`` class containing database connection
        configuration.
    df : pandas.DataFrame
        DataFrame containing the records to be written to the database.
    schema : str
        Target database schema.
    table_name : str
        Name of the table to append the data to.

    Returns
    -------
    bool
        Returns ``True`` if the write operation completes successfully.

    Raises
    ------
    Exception
        Propagates any exception raised during the database write operation.

    Notes
    -----
    - Data is written using ``pandas.DataFrame.to_sql``.
    - Rows are inserted in chunks of 5000 to reduce memory pressure.
    - The database engine is disposed after completion to release resources.
    """

    engine = None
    try:
        engine = None

        log.debug(
            f"Writing a data frame to {schema}.{table_name}, the df has {df.shape[0]} rows"
        )

        if self.db_type == "clickhouse":
            _clickhouse_write(self, df, table_name)
            return True

        engine = _connect(self)

        with engine.begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                if_exists="append",
                schema=schema,
                index=False,
                chunksize=5000,
            )

        return True

    except Exception:
        raise
    finally:
        if engine is not None:
            engine.dispose()


def write_chunk(db_config, df_chunk, schema, table_name):
    from .query import Query

    query = Query(**db_config)
    query._sql_write(df_chunk, schema=schema, table_name=table_name)


def df_split(df, n_splits):
    """
    Split a DataFrame into evenly sized chunks.

    Parameters
    ----------
    df : DataFrame-like
        DataFrame to be split. The object must implement ``to_pandas()``.
    n_splits : int
        Number of partitions to divide the DataFrame into.

    Returns
    -------
    list[pandas.DataFrame]
        List of DataFrame chunks of approximately equal size.

    Notes
    -----
    This function converts the input to a pandas DataFrame before splitting.
    """

    df = df.to_pandas()
    chunk_size = ceil(len(df) / n_splits)
    return [df.iloc[i * chunk_size : (i + 1) * chunk_size] for i in range(n_splits)]


def sql_write(self, df, schema, table_name, max_chunk=10000, max_workers=10, sequential=False):
    """
    Write a DataFrame to a database table.

    The dataset can be written sequentially or split into multiple chunks
    that are processed in parallel using multiprocessing.

    Parameters
    ----------
    self : Query
        Instance of the ``Query`` class containing database configuration.
    df : DataFrame-like
        Dataset to write. Must provide ``shape`` and ``to_pandas()`` methods.
    schema : str
        Target database schema.
    table_name : str
        Name of the destination table.
    max_chunk : int, default 10000
        Maximum number of rows per chunk when splitting the dataset.
    max_workers : int, default 10
        Maximum number of parallel worker processes.
    sequential : bool, default False
        If ``True``, disables parallel processing and writes the DataFrame
        in a single sequential operation.

    Returns
    -------
    bool
        Returns ``True`` when the write operation completes successfully
        in sequential mode.

    Examples
    --------
    >>> q = Query(...)
    >>> q.sql_write(df, schema="public", table_name="users")   

    Notes
    -----
    - Large datasets are split into chunks before writing.
    - Each chunk is processed in a separate process using
      ``ProcessPoolExecutor``.
    - Worker count is automatically adjusted based on dataset size.
    - Garbage collection is triggered after each chunk to free memory.
    """

    if sequential:
        df = df.to_pandas()
        self._sql_write(df, schema=schema, table_name=table_name)
        return True
    
    splits = ceil(df.shape[0] / max_chunk)

    if splits < max_workers:
        max_workers = splits
    if max_workers > 10:
        max_workers = 10

    log.debug(
        f"Writing {df.shape[0]} rows to {schema}.{table_name} "
        f"using {splits} chunks with {max_workers} workers"
    )


    db_config = dict(
        db=self.db,
        db_schema=self.db_schema,
        db_type=self.db_type,
        db_host=self.db_host,
        db_port=self.db_port,
        db_user=self.db_user,
        db_pass=self.db_pass,
        db_oracle_service=self.oracle_service
    ) 

    df_chunks = df_split(df, n_splits=splits)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(write_chunk, db_config, chunk, schema, table_name)
            for chunk in df_chunks
        ]
        for future in futures:
            future.result()
            collect()

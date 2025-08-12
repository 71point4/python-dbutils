import pandas as pd
from ._utils import _connect
import logging
from math import ceil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from gc import collect

log = logging.getLogger(__name__)


def _sql_write(self, df, schema, table_name):
    """
    Write to a database
    """
    try:
        engine = _connect(self)

        log.debug(
            f"Writing a data frame to {schema}.{table_name}, the df has {df.shape[0]} rows"
        )

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
        engine.dispose()


def write_chunk(database, df_chunk, schema, table_name):
    database._sql_write(df_chunk, schema=schema, table_name=table_name)


def df_split(df, n_splits):
    df = df.to_pandas()
    chunk_size = ceil(len(df) / n_splits)
    return [df.iloc[i * chunk_size : (i + 1) * chunk_size] for i in range(n_splits)]


def sql_write(self, df, schema, table_name, max_chunk=10000, max_workers=10, sequential=False):
    """
    Write to a database
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
        f"Writing a data frame to {schema}.{table_name}, the df has {df.shape[0]} rows usings splits = {splits}, and workers = {max_workers}"
    )

    df_chunks = df_split(df, n_splits=splits)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(write_chunk, self, chunk, schema, table_name)
            for chunk in df_chunks
        ]
        for future in futures:
            future.result()
            collect()

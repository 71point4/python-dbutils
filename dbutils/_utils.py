from sqlalchemy import create_engine
import logging

log = logging.getLogger(__name__)

def _connect(self):
    """
    Create a database connection using SQLAlchemy.

    This internal helper builds a SQLAlchemy engine based on the configured
    database credentials and backend type. It supports multiple database
    systems by dynamically constructing the appropriate connection string.

    Supported database types include MySQL, Doris, PostgreSQL/Greenplum,
    and ClickHouse.

    Parameters
    ----------
    self : Query
        Instance of the ``Query`` class containing database configuration
        attributes such as host, port, user credentials, and database name.

    Returns
    -------
    sqlalchemy.engine.Engine
        A SQLAlchemy engine object representing the database connection.

    Raises
    ------
    Exception
        Raised if the connection attempt fails. The error message from the
        underlying database driver will be printed.

    Notes
    -----
    The connection string is generated dynamically based on ``self.db_type``:

    - ``mysql`` → MySQL using ``pymysql``
    - ``doris`` → Doris via MySQL protocol
    - ``psql`` or ``greenplum`` → PostgreSQL using ``psycopg2``
    - ``clickhouse`` → ClickHouse native protocol

    Logging messages are emitted at debug level during the connection process.
    """

    log.debug(
        f'Connecting to {self.db_host}:{self.db_port} for db_type {self.db_type} and user {self.db_user} to {self.db}'
    )

    db_connection = True
    db_connection_str = True

    if self.db_type in ("mysql", "doris"):
        db_connection_str = f'mysql+pymysql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db}'

    if self.db_type in ("greenplum", "psql"):
        db_connection_str = f'postgresql+psycopg2://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db}'
    
    if self.db_type == "clickhouse":
        db_connection_str = f'clickhouse+native://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db}'

    db_connection = create_engine(
        db_connection_str, 
        echo = False,
        )
    
    try:
        with db_connection.connect() as connection:
            log.debug("Connected to database successfully!")
            return db_connection
    except Exception as e:
        log.error(f"Database connection failed: {e}")
        raise


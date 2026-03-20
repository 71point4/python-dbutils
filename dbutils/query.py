import logging
from importlib.metadata import version

class Query:
    """
    Utility class for executing SQL queries and writing data to a database.

    This class stores database connection configuration and exposes helper
    methods for querying and writing data.

    Parameters
    ----------
    db : str, optional
        Name of the target database.
    db_schema : str, default "public"
        Default schema used when executing queries.
    db_type : str
        Type of database backend (for example: ``postgres``, ``oracle``).
    db_host : str
        Database host. Typically supplied via environment variables.
    db_port : str
        Port on which the database server is listening. Typically supplied
        via environment variables.
    db_user : str
        Username used for authentication. Usually provided via environment
        variables.
    db_pass : str
        Password used for authentication. Usually provided via environment
        variables.
    db_oracle_service : str, optional
        Oracle service name if connecting to an Oracle database.

    Attributes
    ----------
    db : str
        Database name.
    db_schema : str
        Active schema used when executing queries.
    db_type : str
        Database backend type. Supported values include
        ``mysql``, ``doris``, ``psql``, ``greenplum`` and ``clickhouse``.
    db_host : str
        Database host address.
    db_port : str
        Database port.
    db_user : str
        Database username.
    db_pass : str
        Database password.
    oracle_service : str
        Oracle service name (if applicable).

    Methods
    -------
    sql_query(...)
        Execute a SQL query and return results.

    sql_write(...)
        Write data to a database table.

    _sql_write(...)
        Internal helper used by ``sql_write`` for database write operations.

    Notes
    -----
    Database credentials are expected to be provided through environment
    variables or external configuration rather than hard coded values.
    """

    __version__ = version("dbutils")

    def __init__(self, **kwargs: dict) -> None:

        kwargs.setdefault("db", False)
        kwargs.setdefault("db_schema", "public")
        kwargs.setdefault("db_oracle_service", False)

        required = ["db_type", "db_host", "db_port", "db_user", "db_pass"]
        missing = [k for k in required if k not in kwargs]

        if missing:
            raise ValueError(f"Missing required database parameters: {missing}")

        self.db = kwargs["db"]
        self.db_schema = kwargs["db_schema"]
        self.db_type = kwargs["db_type"]
        self.db_host = kwargs["db_host"]
        self.db_port = kwargs["db_port"]
        self.db_user = kwargs["db_user"]
        self.db_pass = kwargs["db_pass"]
        self.oracle_service = kwargs["db_oracle_service"]

    from ._sql_query import sql_query
    from ._sql_write import _sql_write
    from ._sql_write import sql_write
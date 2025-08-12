import logging
from pkg_resources import get_distribution


class Query:
    """
    Initialization method of the :code:`Query` class.
    Attributes
    ----------
    db : str
        The name of the database.
    db_schema : str
        The name of the schema
    db_host : str
        Host of DB. [USE ENVIRONMENT VARIABLES]
    db_port : str
        Port where DB listening.[USE ENVIRONMENT VARIABLES]
    db_user : str
        Username [USE ENVIRONMENT VARIABLES].
    db_pass : str
        Password. [USE ENVIRONMENT VARIABLES]
    Methods
    -------
    Fill this space
    """

    def __init__(self, **kwargs):
        __version__ = get_distribution("dbutils").version
        kwargs.setdefault("db", False)
        kwargs.setdefault("db_schema", "public")
        kwargs.setdefault("db_oracle_service", False)

        self.db = kwargs["db"]
        self.db_schema = kwargs["db_schema"]
        self.db_type = kwargs["db_type"]
        self.db_host = kwargs["db_host"]
        self.db_port = kwargs["db_port"]
        self.db_user = kwargs["db_user"]
        self.db_pass = kwargs["db_pass"]
        self.oracle_service = kwargs["db_oracle_service"]

    from ._sql_query import sql_query
    from ._sql_write import sql_write

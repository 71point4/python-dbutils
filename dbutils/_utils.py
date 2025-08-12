from sqlalchemy import create_engine
import logging
import psycopg2
import clickhouse_connect

log = logging.getLogger(__name__)

def _connect(self):
    """
    Connect to Database
    """

    log.debug(
        f'Connecting to {self.db_host}:{self.db_port} for db_type {self.db_type} and user {self.db_user} to {self.db}'
    )

    db_connection = True
    db_connection_str = True

    if self.db_type == "mysql":
        db_connection_str = f'mysql+pymysql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db}'

    if self.db_type == "doris":
        db_connection_str = f'mysql+pymysql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db}'

    if self.db_type == "greenplum" or self.db_type == "psql":
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
        print("Error:", e)


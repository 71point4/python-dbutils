import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
VERSION = "0.1.0"
PACKAGE_NAME = "dbutils"
AUTHOR = "Hanjo Odendaal, Juste Nyirimana"
AUTHOR_EMAIL = "hanjo@71point4.com, justenyirimana@gmail.com"
URL = "XXX"
LICENSE = "PVT"
DESCRIPTION = "DB Utils Python Package to Connect to Databases"
LONG_DESCRIPTION = (HERE / "README.md").read_text(encoding="utf-8")
LONG_DESC_TYPE = "text/markdown"

INSTALL_REQUIRES = [
    "setuptools==47.1.1",
    "numpy==1.26.4",
    "pandas==2.2.3",
    "pytest==6.0.1",
    "requests==2.31.0",
    "Sphinx==3.2.1",
    "sphinx-rtd-theme==0.5.0",
    "m2r2==0.2.5",
    "sqlalchemy==2.0.0",
    "pymysql==1.0.2",
    "python-decouple==3.4.0",
    "wheel==0.37.1",
    "psycopg==3.0.8",
    "psycopg2-binary==2.9.10",
    "pyarrow==18.1.0",
    "polars==1.22.0",
    "clickhouse-sqlalchemy==0.3.2",
    "clickhouse-connect==0.8.15",
]

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type=LONG_DESC_TYPE,
    author=AUTHOR,
    license=LICENSE,
    author_email=AUTHOR_EMAIL,
    url=URL,
    install_requires=INSTALL_REQUIRES,
    packages=find_packages(),
)

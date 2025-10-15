from dagster import IOManager, InputContext, OutputContext
from sqlalchemy import create_engine
import polars as pl
import os
import pandas as pd


def connect_mysql(config) -> str:
    return (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}/{config['database']}"
    )


class MysqlIOManager(IOManager):
    def __init__(self, config=None):
        self._config = config or {
            "host": os.getenv("MYSQL_HOST", "de_mysql"),
            "port": int(os.getenv("MYSQL_PORT", 3306)),
            "user": os.getenv("MYSQL_USER", "admin"),
            "password": os.getenv("MYSQL_PASSWORD", "admin123"),
            "database": os.getenv("MYSQL_DATABASE", "brazillian_ecommerce"),
        }

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        # Chưa cần implement vì chủ yếu là extract
        pass

    def load_input(self, context: InputContext):
        pass

    def extract_data(self, sql: str) -> pl.DataFrame:
        """Chạy query và trả về Polars DataFrame (tất cả cột dưới dạng string)."""
        engine = create_engine(connect_mysql(self._config))
        with engine.connect() as connection:
            pandas_df = pd.read_sql(sql, connection)

            # Ép tất cả cột về string để tránh lỗi ArrowTypeError
            pandas_df = pandas_df.astype(str)

            # Convert sang Polars
            df_data = pl.from_pandas(pandas_df)

            return df_data
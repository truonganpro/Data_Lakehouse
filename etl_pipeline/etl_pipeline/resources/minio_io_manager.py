from dagster import IOManager, InputContext, OutputContext
from minio import Minio
import polars as pl
import pandas as pd
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import os
import pyarrow as pa
import pyarrow.parquet as pq

@contextmanager
def connect_minio(config):
    endpoint = config.get("endpoint_url") or os.getenv("MINIO_ENDPOINT")
    access_key = config.get("minio_access_key") or config.get("access_key") or os.getenv("MINIO_ACCESS_KEY")
    secret_key = config.get("minio_secret_key") or config.get("secret_key") or os.getenv("MINIO_SECRET_KEY")

    # Minio client expects host:port (no http://). Strip if user provided full URL.
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        endpoint = endpoint.split("//", 1)[1]

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    try:
        yield client
    finally:
        pass


def make_bucket(client: Minio, bucket_name: str):
    if not bucket_name:
        raise ValueError("Bucket name not configured for MinioIOManager")
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

class MinioIOManager(IOManager):
    def __init__(self, config=None):
        # chấp nhận cả 'bucket' hoặc 'bucket_name' và cả access key tên khác nhau
        self._config = config or {
            "endpoint_url": os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
            "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
            "bucket": os.getenv("DATALAKE_BUCKET", "warehouse"),
        }

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file_{}_{}.parquet".format("_".join(context.asset_key.path), datetime.today().strftime("%Y%m%d%H%M%S"))
        if context.has_partition_key:
            partition_str = str(table) + "_" + context.asset_partition_key
            return os.path.join(key, f"{partition_str}.parquet"), tmp_file_path
        else:
            return f"{key}.parquet", tmp_file_path

    def handle_output(self, context: OutputContext, obj):
        key_name, tmp_file_path = self._get_path(context)

        if isinstance(obj, pl.DataFrame):
            table = pa.Table.from_pandas(obj.to_pandas())
        elif isinstance(obj, pd.DataFrame):
            table = pa.Table.from_pandas(obj)
        else:
            raise ValueError("Unsupported DataFrame type for Parquet conversion.")

        pq.write_table(table, tmp_file_path)

        try:
            bucket_name = self._config.get("bucket") or self._config.get("bucket_name")
            with connect_minio(self._config) as client:
                make_bucket(client, bucket_name)
                client.fput_object(bucket_name, key_name, tmp_file_path)
                os.remove(tmp_file_path)
        except Exception as e:
            raise e

    def load_input(self, context: InputContext):
        bucket_name = self._config.get("bucket") or self._config.get("bucket_name")
        key_name, tmp_file_path = self._get_path(context)

        try:
            with connect_minio(self._config) as client:
                make_bucket(client, bucket_name)
                context.log.info(f"(MinIO load_input) from key_name: {key_name}")
                client.fget_object(bucket_name, key_name, tmp_file_path)
                df_data = pl.read_parquet(tmp_file_path)
                context.log.info(f"(MinIO load_input) Got Polars DataFrame with shape: {df_data.shape}")
                os.remove(tmp_file_path)
                return df_data
        except Exception as e:
            raise e
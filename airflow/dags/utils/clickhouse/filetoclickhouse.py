from airflow.hooks.base_hook import BaseHook
from typing import List, Literal, Dict, Any
import pandas as pd
import logging
import os
import subprocess
from clickhouse_driver import Client


class File2Clickhouse:
    """File2Clikchouse"""

    # Follow https://clickhouse.com/docs/en/interfaces/formats
    SUPPORTED_TYPE = {
        "csv": "CSVWithNames",
        "parquet": "Parquet",
        "native": "Native",
    }

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self._conn_config = self._get_clickhouse_config()

    def _get_clickhouse_config(self):
        connection = BaseHook.get_connection(self.conn_id)
        try:
            return {
                "host": connection.host,
                "port": connection.port,
                "user": connection.login,
                "password": connection.password,
            }
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve ClickHouse connection: {e}")

    def _check_exists_table(self, database: str, table: str) -> bool:
        client = Client(
            host=self._conn_config["host"],
            port=self._conn_config["port"],
            user=self._conn_config["user"],
            password=self._conn_config["password"],
            database=database,
        )
        result = client.execute(f"EXISTS TABLE {database}.{table}")
        return result[0][0] == 1

    def file_to_clickhouse(self, file_path: str, database: str, table: str):

        logging.info(
            f"Starting ClickHouse ingestion for {file_path} into {database}.{table}"
        )
        clickhouse_connection = self._conn_config
        file_type = os.path.basename(file_path).split(".")[-1]
        file_type = os.path.splitext(file_path)[-1].lower().replace(".", "")
        supported_type = self.SUPPORTED_TYPE.keys()

        if file_type not in supported_type:
            assert_support = ", ".join(supported_type)
            raise ValueError(
                f"Unsupported file type: {file_type}. Must be one of {assert_support}"
            )

        # if not self._check_exists_table(database, table):
        #     # Check exists Table
        #     raise Exception(f"Table {database}.{table} not exists")

        command = [
            "clickhouse-client",
            "--host",
            f'{clickhouse_connection["host"]}',
            "--user",
            f'{clickhouse_connection["user"]}',
            "--password",
            f'{clickhouse_connection["password"]}',
            "--port",
            f'{clickhouse_connection["port"]}',
            "-q",
            "INSERT INTO {}.{} FORMAT {}".format(
                database, table, self.SUPPORTED_TYPE[file_type]
            ),
        ]

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        try:
            with open(file_path) as file:
                logging.info(f"Insert file {file_path} to table {database}.{table}")
                subprocess.run(command, stdin=file, check=True)
        except Exception as e:
            raise e

    def ingest_to_clickhouse(
        self,
        data: pd.DataFrame,
        database: str,
        table: str,
        file_path: str = "/tmp/temp.csv",
        chunksize=10_000,
        from_type="csv",
        cleanup=True,
    ):

        # Save data to csv
        if from_type == "csv":
            self.save_data_csv(data, file_path, chunksize)

        try:
            self.file_to_clickhouse(file_path, database, table)
        except Exception as e:
            logging.error(f"Failed to Write file {file_path} to {database}.{table}")
            raise e
        finally:
            if cleanup and os.path.exists(file_path):
                logging.info(f"Cleaning up file {file_path}")
                os.remove(file_path)

    def save_data_csv(self, data: pd.DataFrame, file_path: str, chunksize=10_000):
        logging.info(f"Writing {len(data)} rows to {file_path}")

        if not os.path.exists(file_path):
            os.makedirs(
                os.path.dirname(file_path), exist_ok=True
            )  # incase the directory does not exist
            data.to_csv(file_path, index=False, chunksize=chunksize)
        else:
            data.to_csv(
                file_path, index=False, mode="a", header=False, chunksize=chunksize
            )

    def clickhouse_to_file(
        self, sql_query, file_path="/tmp/tmp-ch/tmp.orc", data_format="orc"
    ):
        format = "ORC"
        if data_format in ("orc", "native"):
            format = str(data_format).upper()
        else:
            raise ValueError(
                f"Data Format {data_format} does not supported! Please Implement it!"
            )
        clickhouse_connection = self._conn_config
        command = [
            "clickhouse-client",
            f"--port={clickhouse_connection['port']}",
            f"--host={clickhouse_connection['host']}",
            f"--user={clickhouse_connection['user']}",
            f"--password={clickhouse_connection['password']}",
            f"--query={sql_query} FORMAT {format}",
        ]
        logging.info(f"[INFO] Exporting data from ClickHouse to local {format} file...")
        if not os.path.exists(file_path):
            os.makedirs(
                os.path.dirname(file_path), exist_ok=True
            )  # incase the directory does not exist
        with open(file_path, "wb") as f:
            subprocess.run(command, stdout=f, check=True)
        logging.info(f"[DONE] ORC written to: {file_path}")
        return file_path


if __name__ == "__main__":
    conn_id = "conn_clickhopuse"
    database = "traffic_flow"
    table = "dim_cam_info"
    file_ch_client = File2Clickhouse(conn_id=conn_id)
    # data = {"col1": [1, 2, 3], "col2": [2, 3, 4]}
    # data = pd.DataFrame(data)
    print(file_ch_client._check_exists_table(database, table))

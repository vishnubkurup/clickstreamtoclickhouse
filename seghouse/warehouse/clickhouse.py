import logging
from typing import Set, List

from clickhouse_driver import Client

from .warehouse import Warehouse
from ..config.data_type import DataType
from ..config import default_table_structure

from ..util import dataframe_util

logger = logging.getLogger(__name__)

SAMPLE_QUERY = "SELECT 1"
DT_TO_CH_DT = {
    DataType.UINT8: "UInt8",
    DataType.UINT16: "UInt16",
    DataType.UINT32: "UInt32",
    DataType.UINT64: "UInt64",
    DataType.UINT256: "UInt256",
    DataType.INT8: "Int8",
    DataType.INT16: "Int16",
    DataType.INT32: "Int32",
    DataType.INT64: "Int64",
    DataType.INT128: "Int128",
    DataType.INT256: "Int256",
    DataType.FLOAT32: "Float32",
    DataType.FLOAT64: "Float64",
    DataType.BOOLEAN: "UInt8",
    DataType.STRING: "String",
    DataType.DATE: "Date",
    DataType.DATETIME: "DateTime",
}


class ClickHouse(Warehouse):
    clickhouse_client: Client
    clickhouse_cluster: str
    created_tables: Set[str]

    def connect(self):
        self.clickhouse_client = Client(
            host=self.conf_dict["host"],
            port=self.conf_dict.get("port", 9000),
            user=self.conf_dict["user"],
            password=self.conf_dict["password"],
        )
        self.clickhouse_cluster = self.conf_dict.get("cluster")
        logger.info("connecting to ClickHouse")
        logger.info(f"Running sample query {SAMPLE_QUERY}")

        result = self.clickhouse_client.execute(SAMPLE_QUERY)
        logger.info(f"Result = {result}")

        self.created_tables = set()
        return True

    # @abstractmethod
    def create_schema(self, schema: str):
        """ Create schema or namespace if does not exist"""
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS {schema}"
        if self.clickhouse_cluster:
            create_db_sql = f"{create_db_sql} ON CLUSTER {self.clickhouse_cluster}"

        result = self.clickhouse_client.execute(create_db_sql)
        logger.debug(f"Creating Database {schema}, result = {result}")

    # @abstractmethod
    def create_table(self, schema: str, table: str, col_types: dict, non_null_columns: List[str]):
        """ Create table if does not exist"""
        if f"{schema}.{table}" in self.created_tables:
            return

        if self.clickhouse_cluster:
            raise Exception("ClickHouse cluster is not yet implemented")
        else:
            column_type_defs = []
            for col_name, col_type in col_types.items():
                column_type_defs.append(self.to_ch_column_def(col_name, col_type, non_null_columns))

            sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}
            (
                {', '.join(column_type_defs)}
            ) ENGINE = ReplacingMergeTree()
            PARTITION BY toDate(timestamp)
            ORDER BY (timestamp, message_id)
            """
        logger.debug(f"Running SQL = {sql}")
        result = self.clickhouse_client.execute(sql)
        logger.debug(f"Creating Table {schema}.{table}, result = {result}")

        self.created_tables.add(f"{schema}.{table}")

    def create_users_table(self, schema: str, col_types: dict, non_null_columns: List[str]):
        """ Create table if does not exist"""
        table = "users"
        if f"{schema}.{table}" in self.created_tables:
            return

        if self.clickhouse_cluster:
            raise Exception("ClickHouse cluster is not yet implemented")
        else:
            column_type_defs = []
            for col_name, col_type in col_types.items():
                column_type_defs.append(
                    self.to_ch_column_def(
                        col_name, col_type, non_null_columns
                    )
                )

            sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}
            (
                {', '.join(column_type_defs)}
            ) ENGINE = ReplacingMergeTree(ver)
            ORDER BY (user_id)
            """
        logger.debug(f"Running SQL = {sql}")
        result = self.clickhouse_client.execute(sql)
        logger.debug(f"Creating Table {schema}.{table}, result = {result}")

        self.created_tables.add(f"{schema}.{table}")

    @staticmethod
    def to_ch_column_def(
            column_name, column_type, non_null_columns=["received_at", "timestamp", "message_id"]
    ):
        ch_type = DT_TO_CH_DT.get(column_type)
        if ch_type is None:
            raise Exception(f"Unable to find ch_type for DT = {column_type}")
        if column_name not in non_null_columns:
            ch_type = f"Nullable({ch_type})"
        return f"{column_name} {ch_type}"

    # @abstractmethod
    def describe_table(self, schema: str, table: str):
        sql = f"DESCRIBE TABLE {schema}.{table}"
        logger.debug(f"Running SQL = {sql}")
        result = self.clickhouse_client.execute(sql)
        col_types = {}
        for x in result:
            col_types[x[0]] = self.ch_type_to_seghouse_type(x[1])
        return col_types

    @staticmethod
    def ch_type_to_seghouse_type(ch_type):
        if "UInt8" in ch_type:
            return DataType.UINT8
        elif "UInt16" in ch_type:
            return DataType.UINT16
        elif "UInt32" in ch_type:
            return DataType.UINT32
        elif "UInt64" in ch_type:
            return DataType.UINT64
        elif "UInt256" in ch_type:
            return DataType.UINT256
        elif "Int8" in ch_type:
            return DataType.INT8
        elif "Int16" in ch_type:
            return DataType.INT16
        elif "Int32" in ch_type:
            return DataType.INT32
        elif "Int64" in ch_type:
            return DataType.INT64
        elif "Int128" in ch_type:
            return DataType.INT128
        elif "Int256" in ch_type:
            return DataType.INT256
        elif "Float32" in ch_type:
            return DataType.FLOAT32
        elif "Float64" in ch_type:
            return DataType.FLOAT64
        elif "UInt8" in ch_type:
            return DataType.BOOLEAN
        elif "String" in ch_type:
            return DataType.STRING
        elif "DateTime" in ch_type:
            return DataType.DATETIME
        elif "Date" in ch_type:
            return DataType.DATE
        else:
            raise Exception(f"unable to convert ch_type {ch_type}")

    def add_column(self, schema: str, table: str, column: str, column_type: DataType, non_null_columns: List[str]):
        sql = f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS {self.to_ch_column_def(column, column_type, non_null_columns)}"
        logger.debug(f"Running SQL = {sql}")
        result = self.clickhouse_client.execute(sql)
        logger.debug(
            f"Adding column to {schema}.{table}, {column}, {column_type} result = {result}"
        )

    def insert_df(self, schema: str, table: str, dataframe):
        df = dataframe.copy()
        col_types = dataframe_util.get_datatypes(df)
        dataframe_util.cast_boolean_to_int(df, col_types)
        # dataframe_util.mark_int_na_to_default(df, col_types)
        # dataframe_util.mark_float_na_to_default(df, col_types)

        table_column_types = self.describe_table(schema, table)
        dataframe_util.add_missing_columns(df, table_column_types)
        logger.debug(f"{table} table_column_types = {table_column_types}")

        df_dicts = df.to_dict("records")
        misfits = dataframe_util.fix_data_types(df, df_dicts, table_column_types)
        for m in misfits:
            m['table_name'] = table
        self.insert_misfits(schema, misfits)

        result = self.clickhouse_client.execute(
            f"INSERT INTO {schema}.{table} VALUES",
            df_dicts,
            types_check=True,
        )
        logger.info(f"Inserting DataFrame in {schema}.{table}, result = {result}")

    def create_misfits_table(self, schema: str):
        table = default_table_structure.MISFITS_TABLE
        if f"{schema}.{table}" in self.created_tables:
            return

        sql = f"""
                            CREATE TABLE IF NOT EXISTS {schema}.{table}
                            (
                                message_id String,
                                table_name String,
                                column_name String,
                                column_value String,
                                expected_data_type String,
                                actual_data_type String 
                            ) ENGINE = ReplacingMergeTree()
                            ORDER BY (message_id, table_name, column_name)
                            """
        logger.debug(f"Running SQL = {sql}")
        result = self.clickhouse_client.execute(sql)
        logger.debug(f"Creating Table {schema}.{table}, result = {result}")

        self.created_tables.add(f"{schema}.{table}")

    def insert_misfits(self, schema: str, misfits: List[dict]):
        if not misfits:
            return

        self.create_misfits_table(schema)

        table = default_table_structure.MISFITS_TABLE
        result = self.clickhouse_client.execute(
            f"INSERT INTO {schema}.{table} VALUES",
            misfits,
            types_check=True,
        )
        logger.info(f"Inserting DataFrame in {schema}.{table}, result = {result}")

    def close(self):
        return

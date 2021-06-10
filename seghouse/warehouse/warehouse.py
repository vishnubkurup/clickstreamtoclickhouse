from abc import ABCMeta, abstractmethod
from typing import List

from ..config.data_type import DataType


class Warehouse(metaclass=ABCMeta):
    """Abstract Warehouse class"""

    conf_dict: dict

    def __init__(self, conf_dict):
        """Should Create connection"""
        self.conf_dict = conf_dict
        self.connect()

    @abstractmethod
    def connect(self):
        return

    @abstractmethod
    def create_schema(self, schema: str):
        """ Create schema or namespace if does not exist"""
        return

    @abstractmethod
    def create_table(self, schema: str, table: str, col_types: dict, non_null_columns: List[str]):
        """ Create table if does not exist"""
        return

    @abstractmethod
    def create_users_table(self, schema: str, col_types: dict, non_null_columns: List[str]):
        """ Create users table if does not exist"""
        return

    @abstractmethod
    def create_misfits_table(self, schema: str):
        """ Create misfits table if does not exist"""
        return

    @abstractmethod
    def insert_misfits(self, schema: str, misfits: List[dict]):
        """ Create misfits table if does not exist"""
        return

    @abstractmethod
    def describe_table(self, chema: str, table: str):
        return

    @abstractmethod
    def add_column(self, schema: str, table: str, column: str, column_type: DataType, non_null_columns: List[str]):
        return

    @abstractmethod
    def insert_df(self, schema: str, table: str, df):
        return

    @abstractmethod
    def close(self):
        return

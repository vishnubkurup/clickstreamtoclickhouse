from .clickhouse import ClickHouse


def get_warehouse(warehouse_conf: dict):
    if "clickhouse" == warehouse_conf["type"]:
        return ClickHouse(warehouse_conf)
    else:
        raise WarehouseError(f"Unable to get warehouse of type {type}")


class WarehouseError(Exception):
    def __init__(self, message):
        self.message = message

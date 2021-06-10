from enum import Enum, unique


@unique
class DataType(Enum):
    "Lists all Data Types"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    UINT256 = "uint256"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    INT128 = "int128"
    INT256 = "int256"
    FLOAT32 = "float"
    FLOAT64 = "double"
    BOOLEAN = "boolean"
    STRING = "string"
    UUID = "uuid"
    DATE = "date"
    DATETIME = "datetime"
    ARRAY = "array"


INT_DATATYPES = (
    DataType.UINT8,
    DataType.UINT16,
    DataType.UINT32,
    DataType.UINT64,
    DataType.UINT256,
    DataType.INT8,
    DataType.INT16,
    DataType.INT32,
    DataType.INT64,
    DataType.INT128,
    DataType.INT256,
)

FLOAT_DATATYPES = (DataType.FLOAT64, DataType.FLOAT32)

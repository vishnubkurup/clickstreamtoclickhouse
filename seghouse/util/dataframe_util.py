import logging

import numpy as np
import pandas as pd

from ..config import event_fields, data_type

logger = logging.getLogger(__name__)


def get_datatypes(df):
    column_names = list(df.columns.values)
    column_datatypes = {}

    for c in column_names:
        first_good_value = first_valid_value(df, c)
        if first_good_value is None:
            # skip this column
            continue

        if df[c].dtype == object and isinstance(first_good_value, str):
            if c in event_fields.TIMESTAMP_FIELDS:
                column_datatypes[c] = data_type.DataType.DATETIME
            else:
                column_datatypes[c] = data_type.DataType.STRING
                df[c] = df[c].astype(str)
        elif df[c].dtype == np.float64 or float == type(first_good_value):
            column_datatypes[c] = data_type.DataType.FLOAT64
        elif df[c].dtype == np.int64 or int == type(first_good_value):
            column_datatypes[c] = data_type.DataType.INT64
        elif df[c].dtype == np.bool_ or bool == type(first_good_value):
            column_datatypes[c] = data_type.DataType.BOOLEAN
        elif "datetime64" in str(df[c].dtype):
            column_datatypes[c] = data_type.DataType.DATETIME
        else:
            raise Exception(
                f"Unknown type. dtype= {df[c].dtype}. column = {c}, first value = {first_good_value}, Python Type = {type(first_good_value)} "
            )

    return column_datatypes


def first_valid_value(df, column):
    """Returns None if no valid value exists else returns value"""
    valid_index = df[column].first_valid_index()
    if valid_index is None:
        return None
    logger.debug(
        f"valid_index = {valid_index}, column = {column}, df[column][{valid_index}] = {df[column][valid_index]}"
    )
    return df[column][valid_index]


def row_count(df):
    """Faster way to get length of df"""
    return len(df.index)


def empty(df):
    return row_count(df) == 0


def mark_nan_to_none(df):
    return df.where(pd.notnull(df), None)


def mark_string_na_to_default(df, col_types):
    for column_name, column_type in col_types.items():
        if column_type == data_type.DataType.STRING:
            df[column_name].fillna("_default", inplace=True)


def mark_int_na_to_default(df, col_types):
    for column_name, column_type in col_types.items():
        if column_type == data_type.DataType.INT64:
            df[column_name].fillna(0, inplace=True)


def mark_float_na_to_default(df, col_types):
    for column_name, column_type in col_types.items():
        if column_type in (data_type.DataType.FLOAT64, data_type.DataType.FLOAT32):
            df[column_name].fillna(0.0, inplace=True)


def cast_boolean_to_int(df, col_types):
    for column_name, column_type in col_types.items():
        if column_type == data_type.DataType.BOOLEAN:
            df[column_name].fillna(False, inplace=True)
            df[column_name] = df[column_name].astype(int)


def add_missing_columns(df, col_types):
    existing_cols = get_datatypes(df)
    for column_name, column_type in col_types.items():
        if column_name not in existing_cols:
            df[column_name] = None


def fix_data_types(df, df_dicts, expected_col_types):
    """ Fixes data types and returns misfits if not able to fix """
    df_col_types = get_datatypes(df)
    misfits = []

    for column_name, column_type in expected_col_types.items():
        if column_name not in df_col_types:
            continue

        if df_col_types[column_name] == expected_col_types[column_name] and not (df[column_name].dtype == object):
            continue
        else:
            if expected_col_types[column_name] == data_type.DataType.STRING:
                cast_to_str(column_name, df_dicts)
            elif expected_col_types[column_name] in data_type.INT_DATATYPES:
                if df_col_types[column_name] in data_type.INT_DATATYPES and df[column_name].dtype == object:
                    misfits = misfits + cast_to_int(column_name, df_dicts)
                elif df_col_types[column_name] in data_type.FLOAT_DATATYPES:
                    misfits = misfits + cast_to_int(column_name, df_dicts)
                elif df_col_types[column_name] == data_type.DataType.STRING:
                    misfits = misfits + cast_to_int(column_name, df_dicts)
                elif df_col_types[column_name] in data_type.INT_DATATYPES:
                    # Let us hope similar integers will be handled wisely by downstream
                    continue
                else:
                    raise Exception(
                        f"Dont know how to handle. Column = {column_name}, Expected {expected_col_types[column_name]}, Actual {df_col_types[column_name]}"
                    )
            elif expected_col_types[column_name] in data_type.FLOAT_DATATYPES:
                if df_col_types[column_name] in data_type.FLOAT_DATATYPES and df[column_name].dtype == object:
                    misfits = misfits + cast_to_float(column_name, df_dicts)
                elif df_col_types[column_name] in data_type.INT_DATATYPES:
                    misfits = misfits + cast_to_float(column_name, df_dicts)
                elif df_col_types[column_name] == data_type.DataType.STRING:
                    misfits = misfits + cast_to_float(column_name, df_dicts)
                elif df_col_types[column_name] in data_type.FLOAT_DATATYPES:
                    # Let us hope similar integers will be handled wisely by downstream
                    continue
                else:
                    raise Exception(
                        f"Dont know how to handle. Column = {column_name}, Expected {expected_col_types[column_name]}, Actual {df_col_types[column_name]}"
                    )
            else:
                raise Exception(
                    f"Dont know how to handle. Column = {column_name}, Expected {expected_col_types[column_name]}, Actual {df_col_types[column_name]}"
                )
    return misfits


def cast_to_float(column_name, df_dicts):
    misfits = []
    for d in df_dicts:
        if d[column_name] is not None:
            try:
                d[column_name] = float(d[column_name])
            except ValueError:
                misfits.append({'message_id': d['message_id'],
                                'column_name': column_name,
                                'column_value': d[column_name],
                                'expected_data_type': str(float),
                                'actual_data_type': str(type(d[column_name]))
                                })
                d[column_name] = None
    return misfits


def cast_to_str(column_name, df_dicts):
    for d in df_dicts:
        if d[column_name] is not None:
            d[column_name] = str(d[column_name])


def cast_to_int(column_name, df_dicts):
    misfits = []
    for d in df_dicts:
        if d[column_name] is not None:
            try:
                d[column_name] = int(d[column_name])
            except ValueError:
                misfits.append({'message_id': d['message_id'],
                                'column_name': column_name,
                                'column_value': d[column_name],
                                'expected_data_type': str(int),
                                'actual_data_type': str(type(d[column_name]))
                                })
                d[column_name] = None
    return misfits

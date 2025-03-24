# -*- coding: utf-8 -*-

import dataclasses
import json
import os

from collections import OrderedDict
from collections import defaultdict
from typing import Mapping

# 3-rd party modules

from icecream import ic
import numpy as np
import pandas as pd

# local

from .. constants import (
    STAGING_FIELD,
)

from .. config import (
    AssignIdConfig,
    Config,
)

from . search_column_value import search_column_value
from . set_nested_field_value import set_nested_field_value
from . set_row_value import set_row_staging_value

from .. types import (
    AssignIdConfig,
    IdContextMap,
    Row,
)

def get_key_value(
    row: Row,
    primary: list[str],
    context: list[str],
):
    if not primary:
        raise ValueError('Primary columns must be specified')
    context_columns = []
    context_values = []
    if context:
        for context_column in context:
            value, found = search_column_value(row.nested, context_column)
            if not found:
                raise KeyError(f'Column not found: {context_column}, existing columns: {row.flat.keys()}')
            context_columns.append(context_column)
            context_values.append(value)
    primary_columns = []
    primary_values = []
    for primary_column in primary:
        value, found = search_column_value(row.nested, primary_column)
        if not found:
            raise KeyError(f'Column not found: {primary_column}, existing columns: {row.flat.keys()}')
        primary_columns.append(primary_column)
        primary_values.append(value)
    context_key = (
        tuple(context_columns),
        tuple(context_values),
        tuple(primary_columns),
    )
    primary_value = tuple(primary_values)
    return context_key, primary_value

def get_id(
    id_context_map: IdContextMap,
    row: Row,
    primary: list[str],
    context: list[str],
):
    context_key, primary_value = get_key_value(
        row=row,
        primary=primary,
        context=context,
    )
    id_map = id_context_map[context_key]
    #ic(primary_value, context_key)
    #ic(primary_value in id_map.dict_value_to_id)
    if primary_value not in id_map.dict_value_to_id:
        field_id = id_map.max_id + 1
        id_map.max_id = field_id
        id_map.dict_value_to_id[primary_value] = field_id
        id_map.dict_id_to_value[field_id] = primary_value
        id_exists = False
    else:
        field_id = id_map.dict_value_to_id[primary_value]
        id_exists = True
    return field_id, id_exists


def set_id(
    id_context_map: IdContextMap,
    row: Row,
    primary: list[str],
    context: list[str],
    id_value: int,
):
    context_key, primary_value = get_key_value(
        row=row,
        primary=primary,
        context=context,
    )
    id_map = id_context_map[context_key]
    if id_value in id_map.dict_id_to_value:
        #raise ValueError(f'ID already exists: {id_value}')
        field_id = id_map.dict_id_to_value[id_value]
        if field_id != primary_value:
            raise ValueError(f'ID already exists: {id_value} for {field_id}')
    id_map.dict_value_to_id[primary_value] = id_value
    id_map.dict_id_to_value[id_value] = primary_value
    id_map.max_id = max(id_map.max_id, id_value)
    return


def assign_id(
    id_context_map: IdContextMap,
    row: Row,
    config: Config,
):
    if config.reverse:
        #ic(row)
        #ic(type(row))
        #ic(config)
        value, found = search_column_value(row.nested, config.target)
        #ic(row, value, found)
        if type(value) is str:
            if value.isdigit():
                value = int(value)
        if type(value) is int:
            field_id = value
            set_id(
                id_context_map=id_context_map,
                row=row,
                primary=config.primary,
                context=config.context,
                id_value=field_id,
            )
            #return field_id, True
            return row
    field_id, id_exists = get_id(
        id_context_map=id_context_map,
        row=row,
        primary=config.primary,
        context=config.context,
    )
    #ic(config, field_id, id_exists)
    set_row_staging_value(row, config.target, field_id)
    return row

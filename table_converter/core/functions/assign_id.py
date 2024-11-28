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
from .set_nested_field_value import set_nested_field_value

type ContextColumnTuple = tuple[str]
type ContextValueTuple = tuple 
type PrimaryColumnTuple = tuple[str]
type PrimaryValueTuple = tuple

@dataclasses.dataclass
class IdMap:
    max_id: int = 0
    dict_value_to_id: Mapping[PrimaryValueTuple, int] = \
        dataclasses.field(default_factory=defaultdict)
    dict_id_to_value: Mapping[int, PrimaryValueTuple] = \
        dataclasses.field(default_factory=defaultdict)

type IdContextMap = Mapping[
    (
        ContextColumnTuple,
        ContextValueTuple,
        PrimaryColumnTuple,
    ),
    IdMap
]

def create_id_context_map() -> IdContextMap:
    return defaultdict(IdMap)

def assign_id(
    row: OrderedDict,
    dict_assignment: Mapping[str, AssignIdConfig],
    id_context_map: IdContextMap,
):
    new_row = OrderedDict(row)
    for column, config in dict_assignment.items():
        context_columns = []
        context_values = []
        if config.context:
            for context_column in config.context:
                value, found = search_column_value(new_row, context_column)
                if not found:
                    raise KeyError(f'Column not found: {context_column}, existing columns: {new_row.keys()}')
                context_columns.append(context_column)
                context_values.append(value)
        primary_columns = []
        primary_values = []
        for primary_column in config.primary:
            value, found = search_column_value(new_row, primary_column)
            if not found:
                raise KeyError(f'Column not found: {primary_column}, existing columns: {new_row.keys()}')
            primary_columns.append(primary_column)
            primary_values.append(value)
        context_key = (
            tuple(context_columns),
            tuple(context_values),
            tuple(primary_columns),
        )
        primary_value = tuple(primary_values)
        id_map = id_context_map[context_key]
        if primary_value not in id_map.dict_value_to_id:
            field_id = id_map.max_id + 1
            id_map.max_id = field_id
            id_map.dict_value_to_id[primary_value] = field_id
            id_map.dict_id_to_value[field_id] = primary_value
        else:
            field_id = id_map.dict_value_to_id[primary_value]
        new_row[f'{STAGING_FIELD}.{column}'] = field_id
    return new_row

def setup_assign_ids(
    config: Config,
    fields_to_assign_ids: str,
):
    if fields_to_assign_ids:
        fields = fields_to_assign_ids.split(',')
        context = []
        for field in fields:
            if '=' in field:
                dst, src = field.split('=')
                config.process.assign_ids[dst] = AssignIdConfig(
                    primary = [src],
                    context = context,
                )
            else:
                raise ValueError(f'Invalid id assignment: {field}')

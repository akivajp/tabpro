'''
Actions are used to transform the data in the table.
'''

import ast
import json
import re

from collections import OrderedDict
from typing import (
    Any,
)

from ...logging import logger

from ..constants import (
    INPUT_FIELD,
    STAGING_FIELD,
)

from ..classes.row import Row

from .types import (
    AssignArrayConfig,
    AssignConfig,
    AssignConstantConfig,
    JoinConfig,
    OmitConfig,
    ParseConfig,
    PickConfig,
    PushConfig,
)

from ..functions.search_column_value import search_column_value

def delete_flat_row_value(
    flat_row: OrderedDict,
    target: str,
):
    prefix = f'{target}.'
    for key in list(flat_row.keys()):
        if key == target or key.startswith(prefix):
            del flat_row[key]

def pop_nested_row_value(
    nested_row: OrderedDict,
    key: str,
    default: Any = None,
):
    keys = key.split('.')
    for key in keys[:-1]:
        if key not in nested_row:
            return default, False
        nested_row = nested_row[key]
    return nested_row.pop(keys[-1], default), True

def pop_row_value(
    row: Row,
    key: str,
    default: Any = None,
):
    delete_flat_row_value(row.flat, key)
    return pop_nested_row_value(row.nested, key, default)

def pop_row_staging(
    row: Row,
    default: Any = None,
):
    return pop_row_value(row, STAGING_FIELD, default)

def remap_columns(
    row: Row,
    list_config: list[PickConfig],
):
    if not list_config:
        list_config = []
        for key in row.staging.keys():
            list_config.append(PickConfig(
                source = key,
                target = key,
            ))
    new_row = Row()
    picked = []
    for config in list_config:
        value, found = row.search(config.source)
        if found:
            new_row[config.target] = value
            picked.append(found)
    for key in row.keys(include_staging=True):
        if key in picked:
            if not key.startswith(f'{STAGING_FIELD}.{INPUT_FIELD}.'):
                continue
        if key in new_row:
            continue
        if isinstance(key, str) and key.startswith(f'{STAGING_FIELD}.'):
            # NOTE: Skip staging fields
            new_row[key] = row[key]
        else:
            input_key = f'{STAGING_FIELD}.{INPUT_FIELD}.{key}'
            if input_key in row:
                value = row[key]
                input_value = row[input_key]
                if value == input_value:
                    # NOTE: Skip if the same value in the input field
                    continue
            # NOTE: Set the unused value to the staging field
            new_row.staging[key] = row[key]
    return new_row

def search_with_operator(
    row: Row,
    source: str,
):
    or_operator = '||'
    null_or_operator = '??'
    operator_group = f'{re.escape(or_operator)}|{re.escape(null_or_operator)}'
    matched = re.split(f'({operator_group})', source, 1)
    #ic(source, matched)
    if len(matched) == 1:
        return search_column_value(row.nested, source)
    matched = map(str.strip, matched)
    left, operator, rest = matched
    value, found = search_column_value(row.nested, left)
    if operator == or_operator:
        if bool(value):
            return value, found
    if operator == null_or_operator:
        if found and value is not None:
            return value, found
    return search_with_operator(row, rest)

def omit_field(
    row: Row,
    config: OmitConfig,
):
    value, found = pop_row_value(row, config.field)
    if not found:
        return row
    if not config.purge:
        if f'{STAGING_FIELD}.{config.field}' not in row.flat:
            #set_row_staging_value(row, config.field, value)
            row.staging[config.field] = value
    return row

def parse(
    row: Row,
    config: ParseConfig,
):
    value, found = search_column_value(row.nested, config.source)
    if config.required:
        if not found:
            raise ValueError(
                f'Required field not found, field: {config.source}'
            )
    if found:
        if config.as_type == 'literal':
            try:
                if type(value) is str:
                    parsed = ast.literal_eval(value)
                else:
                    parsed = value
            except:
                raise ValueError(
                    f'Failed to parse literal: {value}'
                )
        elif config.as_type == 'json':
            try:
                if type(value) is str:
                    parsed = json.loads(value)
                else:
                    parsed = value
            except:
                raise ValueError(
                    f'Failed to parse JSON: {value}'
                )
        elif config.as_type == 'bool':
            if config.assign_default and value in [None, '']:
                value = config.default_value
            if type(value) is bool:
                parsed = value
            elif type(value) is str:
                if value.lower() in ['true', 'yes', 'on', '1']:
                    parsed = True
                elif value.lower() in ['false', 'no', 'off', '0']:
                    parsed = False
                else:
                    raise ValueError(
                        f'Failed to parse bool: {value}'
                    )
            else:
                raise ValueError(
                    f'Failed to parse bool: {value}'
                )
        else:
            raise ValueError(
                f'Unsupported as type: {config.as_type}'
            )
        #set_row_staging_value(row, config.target, parsed)
        row.staging[config.target] = parsed
    return row

def assign_array(
    row: Row,
    config: AssignArrayConfig,
):
    array = []
    for item in config.items:
        value, found = row.search(item.source)
        if found and value is not None:
            array.append(value)
        elif item.optional:
            array.append(None)
    if array:
        row.staging[config.target] = array
    else:
        row.staging[config.target] = None
    return row

from .setup_actions import (
    setup_actions_with_args,
)
from .do_action import (
    do_actions,
)
__all__ = [
    'do_actions',
    'setup_actions_with_args',
]

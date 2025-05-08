'''
Actions are used to transform the data in the table.
'''

import re

from typing import (
    Any,
)

from ..constants import (
    INPUT_FIELD,
    STAGING_FIELD,
)

from ..classes.row import Row

from .types import (
    AssignArrayConfig,
    PickConfig,
)

from ..functions.search_column_value import search_column_value

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

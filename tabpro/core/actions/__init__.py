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

from .types import (
    AssignArrayConfig,
    AssignConfig,
    AssignConstantConfig,
    AssignFormatConfig,
    AssignIdConfig,
    AssignLengthConfig,
    CastConfig,
    FilterConfig,
    GlobalStatus,
    JoinConfig,
    OmitConfig,
    ParseConfig,
    PickConfig,
    PushConfig,
    SplitConfig,
)

from ..classes.row import Row

from .assign_id import assign_id
from .assign_format import assign_format
from ..functions.search_column_value import search_column_value
#from ..functions.set_row_value import (
#    set_row_staging_value,
#)

def do_actions(
    status: GlobalStatus,
    row: Row,
    actions: list[AssignConstantConfig],
):
    for action in actions:
        try:
            row = do_action(status, row, action)
        except Exception as e:
            logger.error('failed with action: %s', action)
            #logger.error('failed with row: %s', row)
            logger.error('failed with row: %s', dict(row.items()))
            if '__file_row_index__' in row.staging:
                file_row_index = row.staging['__file_row_index__']
                logger.error('failed with file row index: %s', file_row_index)
            raise
        if row is None:
            return None
    return row

def do_action(
    status: GlobalStatus,
    row: Row,
    action: AssignConstantConfig,
):
    if isinstance(action, AssignConfig):
        return assign(row, action)
    if isinstance(action, AssignConstantConfig):
        return assign_constant(row, action)
    if isinstance(action, AssignFormatConfig):
        return assign_format(row, action)
    if isinstance(action, AssignIdConfig):
        return assign_id(status.id_context_map, row, action)
    if isinstance(action, AssignLengthConfig):
        return assign_length(row, action)
    if isinstance(action, CastConfig):
        return cast(row, action)
    if isinstance(action, FilterConfig):
        if filter_row(row, action):
            return row
        return None
    if isinstance(action, JoinConfig):
        return join_field(row, action)
    if isinstance(action, ParseConfig):
        return parse(row, action)
    if isinstance(action, OmitConfig):
        return omit_field(row, action)
    if isinstance(action, PushConfig):
        return push_field(row, action)
    if isinstance(action, SplitConfig):
        return split_field(row, action)
    raise ValueError(
        f'Unsupported action: {action}'
    )

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

def assign_constant(
    row: Row,
    config: AssignConstantConfig,
):
    #set_row_staging_value(row, config.target, config.value)
    row.staging[config.target] = config.value
    return row

def split_field(
    row: Row,
    config: SplitConfig,
):
    value, found = search_column_value(row.flat, config.source)
    if found:
        if isinstance(value, str):
            new_value = value.split(config.delimiter)
            new_value = map(str.strip, new_value)
            new_value = list(filter(None, new_value))
            value = new_value
        #set_row_staging_value(row, config.target, value)
        row.staging[config.target] = value
    return row

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
        value, key = search_column_value(row.nested, config.source)
        if key:
            new_row[config.target] = value
            picked.append(key)
    for key in row.keys():
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

def assign(
    row: Row,
    config: AssignConfig,
):
    value, found = search_with_operator(row, config.source)
    if config.required:
        if not found or bool(value) == False:
            raise ValueError(
                'Required field not found or empty, ' +
                f'field: {config.source}, found: {found}, value: {value}'
            )
    if found:
        row.staging[config.target] = value
    else:
        if config.assign_default:
            row.staging[config.target] = config.default_value
    return row

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

def join_field(
    row: Row,
    config: JoinConfig,
):
    value, found = search_column_value(row.nested, config.source)
    if found:
        delimiter = config.delimiter
        if delimiter is None:
            delimiter = ';'
        if delimiter == '\\n':
            delimiter = '\n'
        if isinstance(value, list):
            value = delimiter.join(value)
        #set_row_staging_value(row, config.target, value)
        row.staging[config.target] = value
    return row

def parse(
    row: Row,
    config: AssignConfig,
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

def push_field(
    row: Row,
    config: PushConfig,
):
    source_value, found = search_column_value(row.nested, config.source)
    do_append = False
    if config.condition is None:
        do_append = True
    else:
        condition_value, found = search_column_value(row.nested, config.condition)
        if condition_value:
            do_append = True
    if do_append:
        target_value, found = search_column_value(row.nested, config.target)
        if found:
            array = target_value
        else:
            array = []
            #set_row_staging_value(row, config.target, array)
            row.staging[config.target] = array
        array.append(source_value)
    return row

def assign_length(
    row: Row,
    config: AssignLengthConfig,
):
    value, found = search_column_value(row.nested, config.source)
    if found:
        #set_row_staging_value(row, config.target, len(value))
        row.staging[config.target] = len(value)
    return row

def cast(
    row: Row,
    config: CastConfig,
):
    value, found = search_column_value(row.nested, config.source)
    if config.required:
        if not found:
            raise ValueError(
                f'Required field not found, field: {config.source}'
            )
    if config.as_type == 'bool':
        cast_func = bool
    elif config.as_type == 'int':
        cast_func = int
    elif config.as_type == 'float':
        cast_func = float
    elif config.as_type == 'str':
        cast_func = str
    else:
        raise ValueError(
            f'Unsupported as type: {config.as_type}'
        )
    try:
        casted = cast_func(value)
    except:
        if config.assign_default:
            casted = config.default_value
        else:
            raise ValueError(
                f'Failed to cast: {value}'
            )
    #set_row_staging_value(row, config.target, casted)
    row.staging[config.target] = casted
    return row

def assign_array(
    row: Row,
    config: AssignArrayConfig,
):
    array = []
    for item in config.items:
        value, found = search_column_value(row, item.source)
        if found and value is not None:
            array.append(value)
        elif item.optional:
            array.append(None)
    if array:
        row.staging[config.target] = array
    else:
        row.staging[config.target] = None
    return row

#from .setup_actions import (
#    setup_actions_with_args,
#)
#
#__all__ = [
#    'setup_actions_with_args',
#]

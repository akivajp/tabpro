'''
Actions are used to transform the data in the table.
'''

from collections import OrderedDict
from dataclasses import dataclass
from typing import (
    Any,
)

from icecream import ic

from . config import (
    Config,
    AssignConstantConfig,
    PickConfig,
    SplitConfig,
)

from . constants import (
    INPUT_FIELD,
    STAGING_FIELD,
)

from . functions.flatten_row import flatten_row
from . functions.nest_row import nest_row
from . functions.search_column_value import search_column_value
from .functions.set_nested_field_value import set_nested_field_value

@dataclass
class Row:
    flat: OrderedDict
    nested: OrderedDict

def setup_actions_with_args(
    config: Config,
    list_actions: list[str],
):
    ic(list_actions)
    for str_action in list_actions:
        fields = str_action.split(':')
        if len(fields) not in [2,3]:
            raise ValueError(
                f'Action must have 2 or 3 colon-separated fields: {str_action}'
            )
        action_name = fields[0].strip()
        str_fields = fields[1].strip()
        if len(fields) == 3:
            str_options = fields[2].strip()
        else:
            str_options = ''
        options = OrderedDict()
        if str_options:
            for str_option in str_options.split(','):
                if '=' in str_option:
                    key, value = str_option.split('=')
                    options[key.strip()] = value.strip()
                else:
                    options[str_option.strip()] = True
        fields = str_fields.split(',')
        for field in fields:
            if '=' in field:
                target, source = field.split('=')
                target = target.strip()
                source = source.strip()
            else:
                target = field.strip()
                source = field.strip()
            if action_name == 'assign-constant':
                str_type = options.get('type', 'str')
                if str_type in ['str', 'string']:
                    value = source
                elif str_type in ['int', 'integer']:
                    value = int(source)
                elif str_type == 'float':
                    value = float(source)
                elif str_type in ['bool', 'boolean']:
                    value = bool(source)
                else:
                    raise ValueError(
                        f'Unsupported type: {str_type}'
                    )
                config.actions.append(AssignConstantConfig(
                    target = target,
                    value = value,
                ))
                continue
            if action_name == 'split':
                delimiter = options.get('delimiter', None)
                config.actions.append(SplitConfig(
                    target = target,
                    source = source,
                    delimiter = delimiter,
                ))
                continue
            raise ValueError(
                f'Unsupported action: {action_name}'
            )
    return config

def do_actions(
    row: Row,
    actions: list[AssignConstantConfig],
):
    for action in actions:
        row = do_action(row, action)
    return row

def do_action(
    row: Row,
    action: AssignConstantConfig,
):
    if isinstance(action, AssignConstantConfig):
        return assign_constant(row, action)
    if isinstance(action, SplitConfig):
        return split_field(row, action)
    raise ValueError(
        f'Unsupported action: {action}'
    )

def prepare_row(
    flat_row: OrderedDict | None = None,
):
    if flat_row is None:
        flat_row = OrderedDict()
    nested_row = nest_row(flat_row)
    return Row(
        flat = OrderedDict(flat_row),
        nested = nested_row,
    )

def set_flat_field_value(
    flat_row: OrderedDict,
    target: str,
    value: Any,
    depth: int = 0,
):
    if depth > 10:
        raise ValueError(
            'Depth too high'
        )
    if isinstance(value, dict):
        for key in value.keys():
            set_flat_field_value(flat_row, f'{target}.{key}', value[key], depth + 1)
    else:
        flat_row[target] = value
    return flat_row

def set_row_value(
    row: Row,
    target: str,
    value: Any,
):
    set_flat_field_value(row.flat, target, value)
    set_nested_field_value(row.nested, target, value)
    return row

def set_row_staging_value(
    row: Row,
    target: str,
    value: Any,
):
    set_row_value(row, f'{STAGING_FIELD}.{target}', value)
    return row

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
            return default
        nested_row = nested_row[key]
    return nested_row.pop(keys[-1], default)

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
    set_row_staging_value(row, config.target, config.value)
    return row

def split_field(
    row: Row,
    config: SplitConfig,
):
    value, found = search_column_value(row.flat, config.source)
    if found:
        if isinstance(value, str):
            new_value = value.split(config.delimiter)
            new_value = list(filter(None, new_value))
            value = new_value
        set_row_staging_value(row, config.target, value)
    return row

def remap_columns(
    row: Row,
    list_config: list[PickConfig],
):
    if not list_config:
        list_config = []
        for key in row.nested[STAGING_FIELD][INPUT_FIELD].keys():
            list_config.append(PickConfig(
                source = key,
                target = key,
            ))
    new_flat_row = OrderedDict()
    picked = []
    for config in list_config:
        value, key = search_column_value(row.nested, config.source)
        if key:
            set_flat_field_value(new_flat_row, config.target, value)
            picked.append(key)
    for key in row.flat.keys():
        if key in picked:
            if not key.startswith(f'{STAGING_FIELD}.{INPUT_FIELD}.'):
                continue
        if key in new_flat_row:
            continue
        if key.startswith(f'{STAGING_FIELD}.'):
            new_flat_row[key] = row.flat[key]
        else:
            new_flat_row[f'{STAGING_FIELD}.{key}'] = row.flat[key]
    row.flat = new_flat_row
    row.nested = nest_row(new_flat_row)
    return row

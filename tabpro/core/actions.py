'''
Actions are used to transform the data in the table.
'''

import ast
import json
import re

from collections import OrderedDict
from dataclasses import dataclass
from typing import (
    Any,
)

from icecream import ic

from . config import (
    Config,
)

from . constants import (
    INPUT_FIELD,
    STAGING_FIELD,
)

from . types import (
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
    Row,
)

from . functions.assign_id import assign_id
from . functions.flatten_row import flatten_row
from . functions.nest_row import nest_row
from . functions.search_column_value import search_column_value
from . functions.set_flat_field_value import set_flat_field_value
from . functions.set_row_value import (
    set_row_staging_value,
)
from . functions.set_nested_field_value import set_nested_field_value

def setup_actions_with_args(
    config: Config,
    list_actions: list[str],
    action_delimiter: str = ':',
):
    ic(list_actions)
    for str_action in list_actions:
        fields = str_action.split(action_delimiter)
        if len(fields) >= 1:
            action_name = fields[0].strip()
        if action_name == 'assign-format':
            setup_assign_format_action(config, str_action, action_delimiter)
            continue
        if action_name == 'filter':
            setup_filter_action(config, str_action, action_delimiter)
            continue
        if len(fields) not in [2,3]:
            raise ValueError(
                'Action must have 2 or 3 delimiter-separated fields: ' +
                f'delimiter:{action_delimiter!r}, action string: {str_action!r}'
            )
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
            if action_name == 'assign':
                assign_default = False
                default_value = None
                if 'default' in options:
                    assign_default = True
                    default_value = options['default']
                if default_value in ['None', 'none', 'Null', 'null']:
                    default_value = None
                required = options.get('required', False)
                config.actions.append(AssignConfig(
                    target = target,
                    source = source,
                    assign_default = assign_default,
                    default_value = default_value,
                    required = required,
                ))
                continue
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
            if action_name == 'assign-id':
                context = options.get('context', None)
                if context:
                    context = context.split(',')
                reverse = options.get('reverse', False)
                config.actions.append(AssignIdConfig(
                    target = target,
                    primary = [source],
                    context = context,
                    reverse = reverse,
                ))
                continue
            if action_name == 'assign-length':
                config.actions.append(AssignLengthConfig(
                    target = target,
                    source = source,
                ))
                continue
            if action_name == 'cast':
                required = options.get('required', False)
                as_type = options.get('as', 'literal')
                if as_type in ['boolean']:
                    as_type = 'bool'
                if as_type not in ['bool', 'int', 'float', 'str']:
                    raise ValueError(
                        f'Unsupported as type: {as_type}'
                    )
                assign_default = False
                default_value = None
                if 'default' in options:
                    assign_default = True
                    default_value = options['default']
                    if default_value in ['None', 'none', 'Null', 'null']:
                        default_value = None
                config.actions.append(CastConfig(
                    target = target,
                    source = source,
                    as_type = as_type,
                    required = required,
                    assign_default = assign_default,
                    default_value = default_value,
                ))
                continue
            if action_name == 'filter-empty':
                config.actions.append(FilterConfig(
                    field = target,
                    operator = 'empty',
                    value = '',
                ))
                continue
            if action_name == 'filter-not-empty':
                config.actions.append(FilterConfig(
                    field = target,
                    operator = 'not-empty',
                    value = '',
                ))
                continue
            if action_name == 'join':
                delimiter = options.get('delimiter', None)
                config.actions.append(JoinConfig(
                    target = target,
                    source = source,
                    delimiter = delimiter,
                ))
                continue
            if action_name == 'omit':
                purge = options.get('purge', False)
                config.actions.append(OmitConfig(
                    field = target,
                    purge = purge,
                ))
                continue
            if action_name == 'parse':
                as_type = options.get('as', 'literal')
                required = options.get('required', False)
                if as_type in ['boolean']:
                    as_type = 'bool'
                if as_type not in ['bool', 'json', 'literal']:
                    raise ValueError(
                        f'Unsupported as type: {as_type}'
                    )
                assign_default = False
                default_value = None
                if 'default' in options:
                    assign_default = True
                    default_value = options['default']
                    if default_value in ['None', 'none', 'Null', 'null']:
                        default_value = None
                config.actions.append(ParseConfig(
                    target = target,
                    source = source,
                    as_type = as_type,
                    required = required,
                    assign_default = assign_default,
                    default_value = default_value,
                ))
                continue
            if action_name == 'parse-json':
                required = options.get('required', False)
                config.actions.append(ParseConfig(
                    target = target,
                    source = source,
                    as_type = 'json',
                    required = required,
                ))
                continue
            if action_name == 'push':
                condition = options.get('condition', None)
                config.actions.append(PushConfig(
                    target = target,
                    source = source,
                    condition = condition,
                ))
                continue
            if action_name == 'split':
                delimiter = options.get('delimiter', None)
                if delimiter == '\\n':
                    delimiter = '\n'
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

def setup_assign_format_action(
    config: Config,
    str_action: str,
    delimiter: str = ':',
):
    action_fields = str_action.split(delimiter, 1)
    if len(action_fields) != 2:
        raise ValueError(
            f'Expected 2 fields separated by ":": {str_action}'
        )
    action_name = action_fields[0].strip()
    assert action_name == 'assign-format'
    assignment_fields = action_fields[1].split('=')
    if len(assignment_fields) != 2:
        raise ValueError(
            f'Expected 2 fields separated by "=": {action_fields[1]}'
        )
    target = assignment_fields[0].strip()
    format = assignment_fields[1].strip()
    config.actions.append(AssignFormatConfig(
        target = target,
        format = format,
    ))
    return config

def setup_filter_action(
    config: Config,
    str_action: str,
    delimiter: str = ':',
):
    action_fields = str_action.split(delimiter, 1)
    if len(action_fields) != 2:
        raise ValueError(
            f'Expected 2 fields separated by ":": {str_action}'
        )
    action_name = action_fields[0].strip()
    assert action_name == 'filter'
    str_filter = action_fields[1].strip()
    if '==' in str_filter:
        field, value = str_filter.split('==')
        config.actions.append(FilterConfig(
            field = field.strip(),
            operator = '==',
            value = value.strip(),
        ))
        return config
    if '!=' in str_filter:
        field, value = str_filter.split('!=')
        config.actions.append(FilterConfig(
            field = field.strip(),
            operator = '!=',
            value = value.strip(),
        ))
        return config
    if '=~' in str_filter:
        field, value = str_filter.split('=~')
        config.actions.append(FilterConfig(
            field = field.strip(),
            operator = '=~',
            value = value.strip(),
        ))
        return config
    raise ValueError(
        f'Unsupported filter: {str_filter}'
    )

def do_actions(
    status: GlobalStatus,
    row: Row,
    actions: list[AssignConstantConfig],
):
    for action in actions:
        row = do_action(status, row, action)
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

def prepare_row(
    flat_row: OrderedDict | None = None,
):
    if flat_row is None:
        flat_row = OrderedDict()
    try:
        nested_row = nest_row(flat_row)
    except:
        ic(flat_row)
        raise
    return Row(
        flat = OrderedDict(flat_row),
        nested = nested_row,
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
            new_value = map(str.strip, new_value)
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
            # NOTE: Skip staging fields
            new_flat_row[key] = row.flat[key]
        else:
            input_key = f'{STAGING_FIELD}.{INPUT_FIELD}.{key}'
            if input_key in row.flat:
                value = row.flat[key]
                input_value = row.flat[input_key]
                if value == input_value:
                    # NOTE: Skip if the same value in the input field
                    continue
            # NOTE: Set the unused value to the staging field
            new_flat_row[f'{STAGING_FIELD}.{key}'] = row.flat[key]
    row.flat = new_flat_row
    row.nested = nest_row(new_flat_row)
    return row


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
        set_row_staging_value(row, config.target, value)
    else:
        if config.assign_default:
            set_row_staging_value(row, config.target, config.default_value)
    return row

def assign_format(
    row: Row,
    config: AssignFormatConfig,
):
    template = config.format
    params = {}
    for key, value in row.flat.items():
        for prefix in [
            f'{STAGING_FIELD}.{INPUT_FIELD}.',
            f'{STAGING_FIELD}.',
        ]:
            if key.startswith(prefix):
                rest = key[len(prefix):]
                params[rest] = value
    params.update(row.flat)
    formatted = None
    while formatted is None:
        try:
            formatted = template.format(**params)
        except KeyError as e:
            #ic(e)
            #ic(e.args)
            #ic(e.args[0])
            key = e.args[0]
            params[key] = f'__{key}__undefined__'
        except:
            #ic(params)
            #ic(params.keys())
            ic(row.flat)
            raise
    set_row_staging_value(row, config.target, formatted)
    return row

def check_empty(
    value: Any,
    found: str | None,
):
    if not found:
        return True
    return not bool(value)

def filter_row(
    row: Row,
    config: list[FilterConfig],
):
    value, found = search_column_value(row.nested, config.field)
    #ic(config, value, found)
    if config.operator == '==':
        if not found:
            return False
        if value != config.value and str(value) != str(config.value):
            return False
    elif config.operator == '!=':
        if str(value) == str(config.value) or value == config.value:
            return False
    elif config.operator == '=~':
        if not found:
            return False
        if not re.search(config.value, value):
            return False
    elif config.operator == 'not-in':
        if isinstance(config.value, list):
            if value in config.value:
                return False
            if str(value) in config.value:
                return False
        else:
            raise ValueError(f'Unsupported filter value type: type{config.value}')
    elif config.operator == 'empty':
        if not check_empty(value, found):
            return False
    elif config.operator == 'not-empty':
        if check_empty(value, found):
            return False
    else:
        raise ValueError(f'Unsupported operator: {config.operator}')
    return True

def omit_field(
    row: Row,
    config: OmitConfig,
):
    value, found = pop_row_value(row, config.field)
    if not found:
        return row
    if not config.purge:
        if f'{STAGING_FIELD}.{config.field}' not in row.flat:
            set_row_staging_value(row, config.field, value)
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
        set_row_staging_value(row, config.target, value)
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
        set_row_staging_value(row, config.target, parsed)
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
            set_row_staging_value(row, config.target, array)
        array.append(source_value)
    return row

def assign_length(
    row: Row,
    config: AssignLengthConfig,
):
    value, found = search_column_value(row.nested, config.source)
    if found:
        set_row_staging_value(row, config.target, len(value))
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
    set_row_staging_value(row, config.target, casted)
    return row

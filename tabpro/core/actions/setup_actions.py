from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from ..config import (
        Config,
    )

from collections import OrderedDict

from ...logging import logger

from .assign import setup_assign_action
from .assign_format import setup_assign_format_action
from .filter_row import setup_filter_action
from .replace_string import setup_replace_action

from . import types

def setup_actions_with_args(
    config: Config,
    list_actions: list[str],
    action_delimiter: str = ':',
):
    logger.debug('list_actions: %s', list_actions)
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
                setup_assign_action(config, target, source, options)
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
                config.actions.append(types.AssignConstantConfig(
                    target = target,
                    value = value,
                ))
                continue
            if action_name == 'assign-id':
                context = options.get('context', None)
                if context:
                    context = context.split(',')
                reverse = options.get('reverse', False)
                config.actions.append(types.AssignIdConfig(
                    target = target,
                    primary = [source],
                    context = context,
                    reverse = reverse,
                ))
                continue
            if action_name == 'assign-length':
                config.actions.append(types.AssignLengthConfig(
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
                config.actions.append(types.CastConfig(
                    target = target,
                    source = source,
                    as_type = as_type,
                    required = required,
                    assign_default = assign_default,
                    default_value = default_value,
                ))
                continue
            if action_name == 'filter-empty':
                config.actions.append(types.FilterConfig(
                    field = target,
                    operator = 'empty',
                    value = '',
                ))
                continue
            if action_name == 'filter-not-empty':
                config.actions.append(types.FilterConfig(
                    field = target,
                    operator = 'not-empty',
                    value = '',
                ))
                continue
            if action_name == 'join':
                delimiter = options.get('delimiter', None)
                config.actions.append(types.JoinConfig(
                    target = target,
                    source = source,
                    delimiter = delimiter,
                ))
                continue
            if action_name == 'omit':
                purge = options.get('purge', False)
                config.actions.append(types.OmitConfig(
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
                config.actions.append(types.ParseConfig(
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
                config.actions.append(types.ParseConfig(
                    target = target,
                    source = source,
                    as_type = 'json',
                    required = required,
                ))
                continue
            if action_name == 'push':
                condition = options.get('condition', None)
                config.actions.append(types.PushConfig(
                    target = target,
                    source = source,
                    condition = condition,
                ))
                continue
            if action_name in ['replace', 'replace-string']:
                setup_replace_action(config, target, source, options)
                continue
            if action_name == 'split':
                delimiter = options.get('delimiter', None)
                if delimiter == '\\n':
                    delimiter = '\n'
                config.actions.append(types.SplitConfig(
                    target = target,
                    source = source,
                    delimiter = delimiter,
                ))
                continue
            raise ValueError(
                f'Unsupported action: {action_name}'
            )
    return config

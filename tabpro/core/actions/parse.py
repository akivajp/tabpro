from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from ..config import Config
    from ..classes.row import Row

import ast
import json

from ..functions.as_boolean import as_boolean
from .types import ParseConfig

def parse(
    row: Row,
    config: ParseConfig,
):
    value, found = row.search(config.source)
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
        row.staging[config.target] = parsed
    return row

def setup_parse_action(
    config: Config,
    target: str,
    source: str,
    options: dict[str, str|bool],
):
    as_type = options.get('as', 'literal')
    required = as_boolean(options.get('required', False))
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

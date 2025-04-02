from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
)
if TYPE_CHECKING:
    from ..classes.row import Row
    from ..config import Config

import math
import re

from .types import (
    FilterConfig,
)

from ...logging import logger

def filter_row(
    row: Row,
    config: list[FilterConfig],
):
    value, found = row.search(config.field)
    #ic(config, value, found)
    if config.value in ['NaN', 'nan']:
        config.value = math.nan
        #logger.debug('config.value: %s', config.value)
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

def check_empty(
    value: Any,
    found: str | None,
):
    if not found:
        return True
    return not bool(value)

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

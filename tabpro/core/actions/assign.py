from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
)
if TYPE_CHECKING:
    from ..config import Config
    from ..classes.row import Row

import dataclasses

from ..functions.as_boolean import as_boolean
from .types import BaseActionConfig

@dataclasses.dataclass
class AssignConfig(BaseActionConfig):
    target: str
    source: str
    assign_default: bool = False
    default_value: Any = None
    required: bool = False
    ignore_empty: bool = False

def assign(
    row: Row,
    config: AssignConfig,
):
    value, found = row.search(config.source)
    if config.required:
        if not found or bool(value) == False:
            raise ValueError(
                'Required field not found or empty, ' +
                f'field: {config.source}, found: {found}, value: {value}'
            )
    if found:
        if config.ignore_empty:
            if value is None or (hasattr(value, '__len__') and len(value) == 0):
                if config.assign_default:
                    row.staging[config.target] = config.default_value
                return row
        row.staging[config.target] = value
    else:
        if config.assign_default:
            row.staging[config.target] = config.default_value
    return row

def setup_assign_action(
    config: Config,
    target: str,
    source: str,
    options: dict[str, str|bool],
):
    assign_default = False
    default_value = None
    if 'default' in options:
        assign_default = True
        default_value = options['default']
    if default_value in ['None', 'none', 'Null', 'null']:
        default_value = None
    ignore_empty = as_boolean(options.get('ignore-empty', False))
    required = as_boolean(options.get('required', False))
    config.actions.append(AssignConfig(
        target = target,
        source = source,
        assign_default = assign_default,
        default_value = default_value,
        required = required,
        ignore_empty = ignore_empty,
    ))

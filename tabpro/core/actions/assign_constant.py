from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from ..config import Config
    from ..classes.row import Row

from .types import AssignConstantConfig

def assign_constant(
    row: Row,
    config: AssignConstantConfig,
):
    row.staging[config.target] = config.value
    return row

def setup_assign_constant_action(
    config: Config,
    target: str,
    source: str,
    options: dict[str, str|bool],
):
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
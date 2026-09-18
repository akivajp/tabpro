from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from ..config import Config
    from ..classes.row import Row

from ..functions.as_boolean import as_boolean

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
        # NOTE:
        #   以前は bool() で文字列を変換していたため、
        #   'false' や '0' なども非空文字列として True になっていた。
        #   'true' / '1' / 'yes' / 'y' を True とする既存の as_boolean を用いる。
        value = as_boolean(source)
    else:
        raise ValueError(
            f'Unsupported type: {str_type}'
        )
    config.actions.append(AssignConstantConfig(
        target = target,
        value = value,
    ))
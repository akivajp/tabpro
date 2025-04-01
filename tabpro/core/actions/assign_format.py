from __future__ import annotations

from .types import (
    AssignFormatConfig,
)

from typing import (
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from ..classes.row import Row
    from ..config import Config

from ..constants import (
    STAGING_FIELD,
    INPUT_FIELD,
)

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
            #ic(row.flat)
            raise
    #set_row_staging_value(row, config.target, formatted)
    row.staging[config.target] = formatted
    return row

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

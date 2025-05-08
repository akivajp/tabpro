from __future__ import annotations
from typing import (
    TYPE_CHECKING,
)
if TYPE_CHECKING:
    from ..classes.row import Row
    from ..config import Config

from .types import OmitConfig

from ..constants import (
    STAGING_FIELD,
)

from ..functions.as_boolean import as_boolean

def omit_field(
    row: Row,
    config: OmitConfig,
):
    value, found = row.pop(config.field)
    if not found:
        return row
    if not config.purge:
        if f'{STAGING_FIELD}.{config.field}' not in row:
            row.staging[config.field] = value
    return row

def setup_omit_field_action(
    config: Config,
    target: str,
    options: dict[str, str|bool],
):
    purge = as_boolean(options.get('purge', False))
    config.actions.append(OmitConfig(
        field = target,
        purge = purge,
    ))

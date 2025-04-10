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
class ReplaceConfig(BaseActionConfig):
    target: str
    source: str
    old: str
    new: str
    count: int = -1
    recursive: bool = False

def _replace_value(
    value: Any,
    config: ReplaceConfig,
):
    if isinstance(value, str):
        new_value = value.replace(config.old, config.new, config.count)
        value = new_value
    if config.recursive:
        if isinstance(value, list):
            new_value = [_replace_value(v, config) for v in value]
            value = new_value
    return value

def replace_string(
    row: Row,
    config: ReplaceConfig,
):
    value, found = row.search(config.source)
    if found:
        value, found = row.search(config.source)
        if found:
            new_value = _replace_value(value, config)
            row.staging[config.target] = new_value
    return row

def setup_replace_action(
    config: Config,
    target: str,
    source: str,
    options: dict[str, str],
):
    count = -1
    recursive = False
    if 'old' not in options:
        raise ValueError('Missing required option: old')
    old = options['old']
    if 'new' not in options:
        raise ValueError('Missing required option: new')
    new = options['new']
    if 'count' in options:
        count = int(options['count'])
    if 'recursive' in options:
        recursive = as_boolean(options['recursive'])
    config.actions.append(ReplaceConfig(
        target = target,
        source = source,
        old = old,
        new = new,
        count = count,
        recursive = recursive,
    ))

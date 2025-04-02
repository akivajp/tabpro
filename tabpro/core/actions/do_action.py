from ..classes.row import Row

from . import types

from ...logging import logger

from .assign import assign
from .cast import cast
from .assign_constant import assign_constant
from .assign_format import assign_format
from .assign_id import assign_id
from .assign_length import assign_length
from .join_field import join_field
from .filter_row import filter_row
from .push_field import push_field
from .replace_string import replace_string
from .split_field import split_field

def do_actions(
    status: types.GlobalStatus,
    row: Row,
    actions: list[types.BaseActionConfig],
):
    for action in actions:
        try:
            row = do_action(status, row, action)
        except Exception as e:
            logger.error('failed with action: %s', action)
            #logger.error('failed with row: %s', row)
            logger.error('failed with row: %s', dict(row.items()))
            if '__file_row_index__' in row.staging:
                file_row_index = row.staging['__file_row_index__']
                logger.error('failed with file row index: %s', file_row_index)
            raise
        if row is None:
            return None
    return row

def do_action(
    status: types.GlobalStatus,
    row: Row,
    action: types.BaseActionConfig,
):
    if isinstance(action, types.AssignConfig):
        return assign(row, action)
    if isinstance(action, types.AssignConstantConfig):
        return assign_constant(row, action)
    if isinstance(action, types.AssignFormatConfig):
        return assign_format(row, action)
    if isinstance(action, types.AssignIdConfig):
        return assign_id(status.id_context_map, row, action)
    if isinstance(action, types.AssignLengthConfig):
        return assign_length(row, action)
    if isinstance(action, types.CastConfig):
        return cast(row, action)
    if isinstance(action, types.FilterConfig):
        if filter_row(row, action):
            return row
        return None
    if isinstance(action, types.JoinConfig):
        return join_field(row, action)
    if isinstance(action, types.OmitConfig):
        return omit_field(row, action)
    if isinstance(action, types.ParseConfig):
        return parse(row, action)
    if isinstance(action, types.PushConfig):
        return push_field(row, action)
    if isinstance(action, types.ReplaceConfig):
        return replace_string(row, action)
    if isinstance(action, types.SplitConfig):
        return split_field(row, action)
    raise ValueError(
        f'Unsupported action: {action}'
    )

from ..classes.row import Row
from .types import PushConfig

def push_field(
    row: Row,
    config: PushConfig,
):
    source_value, found = row.search(config.source)
    do_append = False
    if config.condition is None:
        do_append = True
    else:
        condition_value, found = row.search(config.condition)
        if condition_value:
            do_append = True
    if do_append:
        target_value, found = row.search(config.target)
        if found:
            array = target_value
        else:
            array = []
            row.staging[config.target] = array
        array.append(source_value)
    return row

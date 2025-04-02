from ..classes.row import Row
from .types import AssignLengthConfig

def assign_length(
    row: Row,
    config: AssignLengthConfig,
):
    #value, found = search_column_value(row.nested, config.source)
    value, found = row.search(config.source)
    if found:
        row.staging[config.target] = len(value)
    return row

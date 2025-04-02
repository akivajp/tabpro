from ..classes.row import Row
from .types import SplitConfig

def split_field(
    row: Row,
    config: SplitConfig,
):
    #value, found = search_column_value(row.flat, config.source)
    value, found = row.search(config.source)
    if found:
        if isinstance(value, str):
            new_value = value.split(config.delimiter)
            new_value = map(str.strip, new_value)
            new_value = list(filter(None, new_value))
            value = new_value
        row.staging[config.target] = value
    return row

from ..classes.row import Row
from .types import JoinConfig

def join_field(
    row: Row,
    config: JoinConfig,
):
    #value, found = search_column_value(row.nested, config.source)
    value, found = row.search(config.source)
    if found:
        delimiter = config.delimiter
        if delimiter is None:
            delimiter = ';'
        if delimiter == '\\n':
            delimiter = '\n'
        if isinstance(value, list):
            value = delimiter.join(value)
        row.staging[config.target] = value
    return row

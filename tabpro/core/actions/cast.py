from ..classes.row import Row
from .types import CastConfig

def cast(
    row: Row,
    config: CastConfig,
):
    #value, found = search_column_value(row.nested, config.source)
    value, found = row.search(config.source)
    if config.required:
        if not found:
            raise ValueError(
                f'Required field not found, field: {config.source}'
            )
    if config.as_type == 'bool':
        cast_func = bool
    elif config.as_type == 'int':
        cast_func = int
    elif config.as_type == 'float':
        cast_func = float
    elif config.as_type == 'str':
        cast_func = str
    else:
        raise ValueError(
            f'Unsupported as type: {config.as_type}'
        )
    try:
        casted = cast_func(value)
    except:
        if config.assign_default:
            casted = config.default_value
        else:
            raise ValueError(
                f'Failed to cast: {value}'
            )
    #set_row_staging_value(row, config.target, casted)
    row.staging[config.target] = casted
    return row

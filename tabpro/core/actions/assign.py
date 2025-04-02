from ..classes.row import Row
from .types import AssignConfig

def assign(
    row: Row,
    config: AssignConfig,
):
    #value, found = search_with_operator(row, config.source)
    value, found = row.search(config.source)
    if config.required:
        if not found or bool(value) == False:
            raise ValueError(
                'Required field not found or empty, ' +
                f'field: {config.source}, found: {found}, value: {value}'
            )
    if found:
        row.staging[config.target] = value
    else:
        if config.assign_default:
            row.staging[config.target] = config.default_value
    return row

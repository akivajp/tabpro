from ..classes.row import Row
from .types import AssignLengthConfig

def assign_length(
    row: Row,
    config: AssignLengthConfig,
):
    #value, found = search_column_value(row.nested, config.source)
    value, found = row.search(config.source)
    if found:
        # NOTE:
        #   str / list / dict などサイズを持つ値のみが対象。
        #   それ以外 (数値やブール値など) では len() が TypeError に
        #   なるため、どのフィールドで起きたか分かる形で報告する。
        try:
            length = len(value)
        except TypeError:
            raise ValueError(
                f'assign_length requires a value with a length '
                f'(str, list, dict, ...), but field '
                f'{config.source} has {type(value).__name__}: {value!r}'
            ) from None
        row.staging[config.target] = length
    return row

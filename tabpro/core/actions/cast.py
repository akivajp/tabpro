from ..classes.row import Row
from .types import CastConfig

def cast(
    row: Row,
    config: CastConfig,
) -> Row:
    '''
    指定フィールドの値を所定の型へ変換し、staging に格納する。

    Args:
        row: 対象の行。
        config: cast アクションの設定。

    Returns:
        変換結果を反映した行。
    '''
    value, found = row.search(config.source)
    if not found:
        if config.required:
            raise ValueError(
                f'Required field not found, field: {config.source}'
            )
        # NOTE:
        #   以前は未検出の場合でも None を変換対象にしていたため、
        #   as=str では文字列 'None' が書き込まれ (無言のデータ破損)、
        #   as=int などでは例外で処理全体が停止していた。
        #   assign アクションと同様、既定値の指定が無ければ何もしない。
        if config.assign_default:
            row.staging[config.target] = config.default_value
        return row
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
    except Exception:
        if config.assign_default:
            casted = config.default_value
        else:
            raise ValueError(
                f'Failed to cast: {value}'
            )
    row.staging[config.target] = casted
    return row

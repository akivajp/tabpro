from ..classes.row import Row
from ..functions.as_boolean import parse_boolean_str
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
        # NOTE:
        #   bool() に文字列を通すと非空文字列は常に True になるため
        #   ('false' や '0' も True)、文字列は parse アクションと同じ
        #   解釈で変換する (CSV / Excel 由来の入力は全て文字列)。
        if isinstance(value, str):
            cast_func = parse_boolean_str
        else:
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
    except Exception as e:
        if config.assign_default:
            casted = config.default_value
        else:
            raise ValueError(
                f'failed to cast {value!r} as {config.as_type}'
            ) from e
    row.staging[config.target] = casted
    return row

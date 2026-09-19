def as_boolean(value):
    """
    Convert a value to a boolean.

    Args:
        value: The value to convert.

    Returns:
        bool: The converted boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'y')
    return bool(value)


# NOTE: parse / cast アクションで文字列の真偽値表現を解釈する際の語彙。
#   両アクションで共通のヘルパーを使い、解釈の揺れを防ぐ。
BOOL_TRUE_TOKENS = frozenset(('true', 'yes', 'on', '1'))
BOOL_FALSE_TOKENS = frozenset(('false', 'no', 'off', '0'))

def parse_boolean_str(value: str) -> bool:
    '''
    文字列の真偽値表現を bool へ変換する。

    Args:
        value: 変換対象の文字列 (前後の空白は無視する)。

    Returns:
        bool: 変換結果。

    Raises:
        ValueError: 真偽値として解釈できない文字列の場合。
            bool() と異なり 'false' や '0' を True にせず、
            未知の語彙も黙って False にしない。
    '''
    token = value.strip().lower()
    if token in BOOL_TRUE_TOKENS:
        return True
    if token in BOOL_FALSE_TOKENS:
        return False
    raise ValueError(f'Failed to parse bool: {value}')

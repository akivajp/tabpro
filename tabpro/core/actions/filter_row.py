from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Any,
)
if TYPE_CHECKING:
    from ..classes.row import Row
    from ..config import Config

import math
import re

from .types import (
    FilterConfig,
)


def filter_row(
    row: Row,
    config: FilterConfig,
):
    value, found = row.search(config.field)
    # NOTE:
    #   config.value は行ごとに書き換えず、ローカル変数に落として扱う。
    #   以前は dataclass の値そのものを NaN に書き換えていた。
    filter_value = config.value
    if filter_value in ['NaN', 'nan']:
        filter_value = math.nan
    if config.operator == '==':
        if not found:
            return False
        if value != filter_value and str(value) != str(filter_value):
            return False
    elif config.operator == '!=':
        if str(value) == str(filter_value) or value == filter_value:
            return False
    elif config.operator == '=~':
        if not found:
            return False
        if not re.search(str(filter_value), str(value)):
            return False
    elif config.operator in ['>', '>=', '<', '<=']:
        if not found:
            return False
        if not compare_ordered(value, filter_value, config.operator):
            return False
    elif config.operator == 'not-in':
        if isinstance(config.value, list):
            if value in config.value:
                return False
            if str(value) in config.value:
                return False
        else:
            raise ValueError(f'Unsupported filter value type: type{config.value}')
    elif config.operator == 'empty':
        if not check_empty(value, found):
            return False
    elif config.operator == 'not-empty':
        if check_empty(value, found):
            return False
    else:
        raise ValueError(f'Unsupported operator: {config.operator}')
    return True

def compare_ordered(
    value: Any,
    filter_value: Any,
    operator: str,
):
    '''
    大小比較 (> >= < <=) を数値として行う。

    どちらか一方でも数値に変換できない場合は、
    黙って文字列比較など別の基準に切り替えるのではなく、
    明確なエラーにして処理を止める (異常データの検知が目的のため)。

    Args:
        value: 行から取得した値。
        filter_value: フィルタ設定の比較値。
        operator: 比較演算子 (> >= < <= のいずれか)。

    Returns:
        比較結果 (行を残すかどうか)。

    Raises:
        ValueError: いずれかの値が数値として解釈できない場合。
    '''
    try:
        left = float(value)
        right = float(filter_value)
    except (TypeError, ValueError) as e:
        raise ValueError(
            f'filter operator {operator} requires numeric values, '
            f'but got field value: {value!r} and filter value: {filter_value!r}'
        ) from e
    if operator == '>':
        return left > right
    if operator == '>=':
        return left >= right
    if operator == '<':
        return left < right
    return left <= right

def check_empty(
    value: Any,
    found: str | None,
):
    if not found:
        return True
    return not bool(value)

def setup_filter_action(
    config: Config,
    str_action: str,
    delimiter: str = ':',
):
    action_fields = str_action.split(delimiter, 1)
    if len(action_fields) != 2:
        raise ValueError(
            f'Expected 2 fields separated by ":": {str_action}'
        )
    action_name = action_fields[0].strip()
    assert action_name == 'filter'
    str_filter = action_fields[1].strip()
    # NOTE:
    #   値に '==' などを含む指定 (例: comment==hello==world) でも
    #   最初の演算子のみで分割するよう maxsplit=1 を付ける。
    #   以前は maxsplit 無しの split だったため、値に演算子が含まれると
    #   "too many values to unpack" でクラッシュしていた。
    if '==' in str_filter:
        field, value = str_filter.split('==', 1)
        config.actions.append(FilterConfig(
            field = field.strip(),
            operator = '==',
            value = value.strip(),
        ))
        return config
    if '!=' in str_filter:
        field, value = str_filter.split('!=', 1)
        config.actions.append(FilterConfig(
            field = field.strip(),
            operator = '!=',
            value = value.strip(),
        ))
        return config
    # NOTE:
    #   '>' よりも先に '>=' を判定する (1文字の区切りが先だと
    #   '>=' が '>' と残り '=...' に誤って分解されるため)。
    for operator in ['>=', '<=']:
        if operator in str_filter:
            field, value = str_filter.split(operator, 1)
            config.actions.append(FilterConfig(
                field = field.strip(),
                operator = operator,
                value = value.strip(),
            ))
            return config
    if '=~' in str_filter:
        field, value = str_filter.split('=~', 1)
        config.actions.append(FilterConfig(
            field = field.strip(),
            operator = '=~',
            value = value.strip(),
        ))
        return config
    for operator in ['>', '<']:
        if operator in str_filter:
            field, value = str_filter.split(operator, 1)
            config.actions.append(FilterConfig(
                field = field.strip(),
                operator = operator,
                value = value.strip(),
            ))
            return config
    raise ValueError(
        f'Unsupported filter: {str_filter}'
    )

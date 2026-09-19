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
            # NOTE:
            #   parse-json で生成された配列には数値などが混ざりうる。
            #   以前は str.join に直接渡していたため、非文字列の要素が
            #   1つでもあると TypeError で停止していた。文字列化して結合する。
            value = delimiter.join(str(item) for item in value)
        row.staging[config.target] = value
    return row

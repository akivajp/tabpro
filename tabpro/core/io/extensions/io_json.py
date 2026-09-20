import json

from typing import Any

from rich.console import Console

from . manage_loaders import (
    Row,
    register_loader,
    detect_text_encoding,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

from ... progress import Progress

from .... logging import logger

# JSON で意味を持つエスケープ文字 (\uXXXX は別途判定する)
VALID_ESCAPE_CHARS = set('"\\/bfnrt')
HEX_DIGITS = set('0123456789abcdefABCDEF')

def is_valid_escape(
    text: str,
    index: int,
) -> bool:
    '''
    text[index] のバックスラッシュが、正しいエスケープの開始かを判定する。

    Args:
        text: 判定対象の文字列。
        index: バックスラッシュの位置。

    Returns:
        正しいエスケープであれば True。
    '''
    if index + 1 >= len(text):
        return False
    following = text[index + 1]
    if following in VALID_ESCAPE_CHARS:
        return True
    if following == 'u':
        digits = text[index + 2:index + 6]
        return len(digits) == 4 and all(c in HEX_DIGITS for c in digits)
    return False

def escape_json(
    str_json: str,
) -> str:
    r'''
    エスケープとして無効なバックスラッシュのみを二重化する。

    エスケープせずに書かれた Windows のパスなど、そのままでは読めない
    JSON を救済するための関数。正しい JSON には適用してはならない。

    NOTE:
        以前は \n, \", \\ 以外を全て無効とみなして二重化していたため、
        \t や \uXXXX といった正しいエスケープまで壊していた。
        json.dumps は既定 (ensure_ascii=True) で非 ASCII を \uXXXX に
        するため、日本語を含む JSON がことごとく壊れていた。

    Args:
        str_json: 対象の JSON 文字列。

    Returns:
        救済を施した JSON 文字列。
    '''
    chars = []
    index = 0
    changed = False
    while index < len(str_json):
        char = str_json[index]
        if char == '\\':
            if is_valid_escape(str_json, index):
                # NOTE: 正しいエスケープはエスケープ対象ごとそのまま写す
                chars.append(str_json[index:index + 2])
                index += 2
                continue
            chars.append('\\\\')
            changed = True
            index += 1
            continue
        chars.append(char)
        index += 1
    if not changed:
        return str_json
    return ''.join(chars)

def loads_json(
    text: str,
) -> Any:
    r'''
    JSON を読み込む。読めない場合に限り、救済を試みる。

    NOTE:
        以前は読み込みの前に無条件で escape_json() を適用していたため、
        正しい JSON まで壊していた。救済は最後の手段として使う。

    Args:
        text: JSON 文字列。

    Returns:
        読み込まれた値。

    Raises:
        json.JSONDecodeError: 救済しても読み込めない場合。
    '''
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        repaired = escape_json(text)
        if repaired == text:
            raise
        logger.debug('recovering malformed json by escaping backslashes')
        return json.loads(repaired)

@register_loader('.json')
def load_json(
    input_file: str,
    progress: Progress | None = None,
    **kwargs,
):
    quiet = kwargs.get('quiet', False)
    if progress is not None:
        console = progress.console
    else:
        console = Console()
    if not quiet:
        console.log('loading json data from: ', input_file)
    # NOTE:
    #   encoding の明示指定が無い場合は CSV/TSV と同じ自動判定に任せる
    #   (utf-8-sig は BOM 無しの UTF-8 も読める)。
    #   明示指定時は厳格に扱う。
    encoding = kwargs.get('encoding')
    if encoding is None:
        encoding = detect_text_encoding(input_file, console=console, quiet=quiet)
    with open(input_file, 'r', encoding=encoding) as f:
        data = loads_json(f.read())
    if not isinstance(data, list):
        raise ValueError(f'invalid json array data: {input_file}')
    for row in data:
        yield Row.from_dict(row)

@register_writer('.json')
class JsonWriter(BaseWriter):
    def __init__(
        self,
        output_file: str,
        **kwargs,
    ):
        super().__init__(output_file, **kwargs)

    def support_streaming(self):
        return False
    
    def _write_all_rows(self):
        self._open()
        # NOTE:
        #   空入力でも空配列の JSON ファイルを出力する。
        #   以前は rows が空だと書き出し自体がスキップされ、
        #   出力ファイルが作られないまま終わっていた。
        rows = [row.nested for row in self.rows]
        if self.fobj:
            self.fobj.write(json.dumps(rows, indent=2, ensure_ascii=False))
            self.fobj.close()
        self.fobj = None
        self.finished = True

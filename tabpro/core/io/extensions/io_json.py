import json
import re

from rich.console import Console

from . manage_loaders import (
    Row,
    register_loader,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

from ... progress import Progress

from .... logging import logger

# 除外対象: 改行、二重引用符のエスケープ、バックスラッシュのエスケープ
# 半角円記号や除外対象以外を追加エスケープ
# 取り急ぎはその他の制御文字については対応なし
#regex_escape = re.compile(r'(\\[^n"\\])')
#regex_replace = r'\\\\\1'
def escape_json(str_json: str) -> str:
    # NOTE: 正規表現で全てカバーするのは厳しそう
    #str_json = re.sub(r'(\\[^n"\\])', '\\\\\\1', str_json)
    # NOTE: 文字単位処理
    chars = list(str_json)
    changed = False
    i = 0
    while i < len(chars):
        char = chars[i]
        if i == len(chars) - 1:
            break
        next_char = chars[i+1]
        if char == '\\':
            if next_char in ['n', '"', '\\']:
                i = i + 2
                continue
            else:
                chars[i] = '\\\\'
                changed = True
        i = i + 1
    if changed:
        str_json = ''.join(chars)
    return str_json

@register_loader('.json')
def load_json(
    input_file: str,
    progress: Progress | None = None,
    **kwargs,
):
    quiet = kwargs.get('quiet', False)
    if not quiet:
        if progress is not None:
            console = progress.console
        else:
            console = Console()
        console.log('loading json data from: ', input_file)
    with open(input_file, 'r') as f:
        str_json = f.read()
        str_json = escape_json(str_json)
        data = json.loads(str_json)
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
        #if not self.quiet:
        #    console = self._get_console()
        #    console.log(f'writing {len(self.rows)} json rows into: ', self.target)
        if self.rows:
            rows = [row.nested for row in self.rows]
            if self.fobj:
                self.fobj.write(json.dumps(rows, indent=2, ensure_ascii=False))
                self.fobj.close()
        self.fobj = None
        self.finished = True

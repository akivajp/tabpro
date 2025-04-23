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

def escape_json(str_json: str) -> str:
    # 改行と二重引用符のエスケープ以外や円記号を追加エスケープ
    # 取り急ぎはその他の制御文字については対応なし
    str_json = re.sub(r'(\\[^n"])', '\\\\\\1', str_json)
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

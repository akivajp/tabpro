import json

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
        data = json.load(f)
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
        rows = [row.nested for row in self.rows]
        self.fobj.write(json.dumps(rows, indent=2, ensure_ascii=False))
        self.fobj.close()
        self.fobj = None
        self.finished = True

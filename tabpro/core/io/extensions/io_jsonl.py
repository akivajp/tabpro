import json

from . manage_loaders import (
    Row,
    register_loader,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

from ... progress import (
    Console,
    Progress,
)

from rich.progress import open as rich_open

from . io_json import escape_json

@register_loader('.jsonl')
def load_jsonl(
    input_file: str,
    progress: Progress | None = None,
    **kwargs,
):
    quiet = kwargs.get('quiet', False)
    if progress is None:
        console = Console()
    else:
        console = progress.console
    if not quiet:
        console.log('Loading from: ', input_file)
        def fn_open(file, *args, **kwargs):
            description = 'Loading JSON rows'
            if progress:
                return progress.open(
                    file,
                    *args,
                    description=description,
                    **kwargs,
                )
            else:
                return rich_open(
                    file,
                    *args,
                    description=description,
                    **kwargs,
                )
    else:
        fn_open = open
    with fn_open(input_file, 'r') as f:
        for line in f:
            line = escape_json(line)
            row = json.loads(line)
            yield Row.from_dict(row)

@register_writer('.jsonl')
class JsonLinesWriter(BaseWriter):
    def __init__(
        self,
        output_file: str,
        **kwargs,
    ):
        super().__init__(output_file, **kwargs)

    def support_streaming(self):
        return True

    def _write_row(self, row: Row):
        if self.fobj:
            self.fobj.write(json.dumps(row.flat, ensure_ascii=False))
            self.fobj.write('\n')

    def _write_all_rows(self):
        if self.rows:
            for row in self.rows:
                self._write_row(row)
        if self.fobj:
            self.fobj.close()

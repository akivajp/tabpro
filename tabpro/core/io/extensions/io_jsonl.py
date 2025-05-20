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
    Progress,
)

from . io_json import escape_json

@register_loader('.jsonl')
def load_jsonl(
    input_file: str,
    progress: Progress | None = None,
    limit: int | None = None,
    **kwargs,
):
    orig_progress = progress
    quiet = kwargs.get('quiet', False)
    if progress is None:
        progress = Progress()
        progress.start()
    open_task_id = None
    if not quiet:
        progress.console.log('Loading from: ', input_file)
        description = 'Reading JSONL file'
        open_task_id = progress.add_task(
            description = description,
        )
        def fn_open(file, *args, **kwargs):
            return progress.open(
                file,
                *args,
                task_id = open_task_id,
                **kwargs,
            )
    else:
        fn_open = open
    count_task_id = progress.add_task(
        description = 'Loaded JSON rows',
        total = limit,
        disable = quiet,
    )
    with fn_open(input_file, 'r') as f:
        for i, line in enumerate(f):
            if limit and i >= limit:
                break
            line = escape_json(line)
            row = json.loads(line)
            progress.update(count_task_id, advance=1)
            yield Row.from_dict(row)
        f.close()
        if open_task_id is not None:
            progress.stop_task(open_task_id)
        progress.stop_task(count_task_id)
    if orig_progress is None:
        progress.stop()

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
        if not self.fobj:
            self._open()
            assert self.fobj is not None
        self.fobj.write(json.dumps(row.nested, ensure_ascii=False))
        self.fobj.write('\n')

    def _write_all_rows(self):
        if self.rows:
            for row in self.rows:
                self._write_row(row)
        if self.fobj:
            self.fobj.close()

import json

from logzero import logger
from tqdm.auto import tqdm

from . manage_loaders import (
    Row,
    register_loader,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

@register_loader('.jsonl')
def load_jsonl(
    input_file: str,
    **kwargs,
):
    quiet = kwargs.get('quiet', False)
    with open(input_file, 'r') as f:
        for line in tqdm(
            f,
            desc=f'Loading JSON Lines from: {input_file}',
        ):
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

    def write_row(self, row: Row):
        self.fobj.write(json.dumps(row.flat, ensure_ascii=False))
        self.fobj.write('\n')

    def _write_all_rows(self):
        for row in self.rows:
            self.write_row(row)
        self.fobj.close()

import json

from logzero import logger

from . manage_loaders import (
    Row,
    register_loader,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

@register_loader('.json')
def load_json(
    input_file: str,
    **kwargs,
):
    quiet = kwargs.get('quiet', False)
    if not quiet:
        logger.info('Loading JSON data from: %s', input_file)
    with open(input_file, 'r') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f'Invalid JSON array data: {input_file}')
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
    
    def write_all_rows(self):
        self.open()
        if not self.quiet:
            logger.info('Writing %s JSON rows into: %s', len(self.rows), self.target)
        rows = [row.nested for row in self.rows]
        self.fobj.write(json.dumps(rows, indent=2, ensure_ascii=False))
        self.fobj.close()
        self.fobj = None
        self.finished = True

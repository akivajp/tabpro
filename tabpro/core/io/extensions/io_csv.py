#import pandas as pd

import csv

from collections import OrderedDict

from rich.console import Console

from tqdm.auto import tqdm

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
    track,
)

@register_loader('.csv')
def load_csv(
    input_file: str,
    progress: Progress | None = None,
    **kwargs,
):
    no_header = kwargs.get('no_header', False)
    # UTF-8 with BOM
    quiet = kwargs.get('quiet', False)
    encoding = kwargs.get('encoding', 'utf-8-sig')
    if progress is None:
        console = Console()
    else:
        console = progress.console
    console.log('Loading CSV data from: ', input_file)
    def get_iter(reader):
        return track(
            reader,
            description='Loading rows...',
            disable=quiet,
            progress=progress,
        )
    reader = csv.reader(open(input_file, 'r', encoding=encoding))
    if no_header:
        for i, row in enumerate(get_iter(reader)):
            d = {}
            for j, field in enumerate(row):
                d[f'{j}'] = field
            yield Row.from_dict(d)
    else:
        for i, row in enumerate(get_iter(reader)):
            if i == 0:
                header = row
                continue
            d = OrderedDict()
            for j, field in enumerate(row):
                d[header[j]] = field
            #for j in range(len(row), len(header)):
            #    d[f'__staging__.__values__.{j}'] = None
            yield Row.from_dict(d)

@register_writer('.csv')
class CsvWriter(BaseWriter):
    def __init__(
        self,
        output_file: str,
        **kwargs,
    ):
        self.writer: csv.DictWriter | None = None
        super().__init__(
            output_file,
            **kwargs,
        )

    def support_streaming(self):
        return True

    def _write_row(self, row: Row):
        if self.writer is None:
            # NOTE: 最初の行を取得するまでヘッダーを決定できない
            self.writer = csv.DictWriter(self.fobj, fieldnames=row.flat.keys())
            self.writer.writeheader()
        self.writer.writerow(row.flat)

    def _write_all_rows(self):
        if self.rows is None:
            return
        self._open()
        self.writer = csv.DictWriter(self.fobj)
        header = []
        for row in self.rows:
            for key in row.keys():
                if key not in header:
                    header.append(key)
        self.writer.writeheader(header)
        for row in self.rows:
            self._write_row(row)
        self.writer = None
        self.fobj.close()
        self.fobj = None

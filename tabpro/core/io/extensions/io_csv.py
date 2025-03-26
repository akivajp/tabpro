#import pandas as pd

import csv

from tqdm.auto import tqdm

from . manage_loaders import (
    Row,
    register_loader,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

@register_loader('.csv')
def load_csv(
    input_file: str,
    **kwargs,
):
    no_header = kwargs.get('no_header', False)
    # UTF-8 with BOM
    quiet = kwargs.get('quiet', False)
    encoding = kwargs.get('encoding', 'utf-8-sig')
    if no_header:
        reader = csv.reader(open(input_file, 'r', encoding=encoding))
        for i, row in enumerate(
            tqdm(reader, desc=f'Loading CSV from: {input_file}', disable=quiet)
        ):
            d = {}
            for j, field in enumerate(row):
                d[f'{j}'] = field
            yield Row.from_dict(d)
    else:
        reader = csv.DictReader(open(input_file, 'r', encoding=encoding))
        for i, row in enumerate(
            tqdm(reader, desc=f'Loading CSV from: {input_file}', disable=quiet)
        ):
            yield Row.from_dict(row)

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

    def open(self):
        self.fobj = open(self.target, 'w', encoding=self.encoding)
        # NOTE: 最初の行を取得するまでヘッダーを決定できない
        #self.writer = csv.writer(self.fobj)

    def write_row(self, row: Row):
        if self.writer is None:
            self.writer = csv.DictWriter(self.fobj, fieldnames=row.flat.keys())
            self.writer.writeheader()
        self.writer.writerow(row.flat)

    def write_all_rows(self):
        if self.rows is None:
            return
        self.open()
        self.writer = csv.DictWriter(self.fobj)
        header = []
        for row in self.rows:
            for key in row.keys():
                if key not in header:
                    header.append(key)
        self.writer.writeheader(header)
        for row in self.rows:
            self.write_row(row)
        self.writer = None
        self.fobj.close()
        self.fobj = None

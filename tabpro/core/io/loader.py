'''
Loader class is responsible for loading the data from the source.
'''

import os.path

from . extensions.manage_loaders import get_loader
from ..classes.row import Row

from .. progress import (
    Progress,
)


class Loader:
    def __init__(
        self,
        source: str,
        quiet: bool = False,
        no_header: bool = False,
        limit: int | None = None,
        progress: Progress | None = None,
        sheet: str | None = None,
        all_sheets: bool = False,
        keep_rows: bool = False,
    ):
        self.source = source
        self.quiet = quiet
        self.no_header = no_header
        self.limit = limit
        # NOTE: Excel 以外のローダーは **kwargs でこれらを読み捨てる
        self.sheet = sheet
        self.all_sheets = all_sheets
        # NOTE:
        #   既定では読み込んだ行を保持しない。
        #   以前は常に全行を溜めていたため、len() を呼んだ時点で
        #   入力サイズに比例したメモリを消費していた。
        #   len() や複数回の走査が必要な場合のみ keep_rows=True にする。
        self.keep_rows = keep_rows
        self.rows: list[Row] | None = None
        self.progress = progress
        self.fn_load = get_loader(
            self.source,
        )
        self.extension = os.path.splitext(self.source)[1]

    def __iter__(self):
        return self._yield_data()
    
    def __len__(self):
        if not self.keep_rows:
            # NOTE:
            #   件数を数えるには全件を読む必要があり、
            #   黙って行うとメモリ消費の原因が見えなくなる。
            #   TypeError にしておくのが重要: list(loader) など
            #   Python 内部の length_hint は __len__ にフォールバックし、
            #   そこから送出された例外は TypeError の場合のみ
            #   「ヒント無し」として無視されるため、全行の
            #   実体化 (list への変換) が正常に行えるようになる。
            raise TypeError(
                'len() requires holding every row in memory. '
                'Construct the loader with keep_rows=True if that is intended, '
                'or count the rows while iterating instead.'
            )
        if self.rows is None:
            for _ in self._yield_data():
                pass
        if self.rows is None:
            raise ValueError('No rows loaded')
        return len(self.rows)

    def _yield_data(self):
        if self.rows is not None:
            # NOTE: keep_rows=True で既に読み込み済みの場合は再読み込みしない
            yield from self.rows
            return
        rows: list[Row] | None = [] if self.keep_rows else None
        for row in self.fn_load(
            self.source,
            quiet=self.quiet,
            no_header=self.no_header,
            progress=self.progress,
            limit=self.limit,
            sheet=self.sheet,
            all_sheets=self.all_sheets,
        ):
            if rows is not None:
                rows.append(row)
            yield row
        if rows is not None:
            self.rows = rows

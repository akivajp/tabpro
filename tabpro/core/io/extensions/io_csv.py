from typing import (
    Generator,
    Iterable,
)

import csv

from collections import OrderedDict

from rich.console import Console

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

def _load_delimited(
    input_file: str,
    delimiter: str,
    label: str,
    progress: Progress | None = None,
    **kwargs,
) -> Generator[Row, None, None]:
    '''
    区切り文字で区切られたテキストファイルを1行ずつ読み込む。

    Args:
        input_file: 入力ファイルのパス。
        delimiter: 区切り文字 (CSV なら ',', TSV なら '\\t')。
        label: ログ表示に用いる形式名。
        progress: 進捗表示に用いる Progress。

    Yields:
        1行分の Row。
    '''
    no_header = kwargs.get('no_header', False)
    quiet = kwargs.get('quiet', False)
    # NOTE: BOM 付き UTF-8 を透過的に扱う
    encoding = kwargs.get('encoding', 'utf-8-sig')
    if progress is None:
        console = Console()
    else:
        console = progress.console
    if not quiet:
        console.log(f'Loading {label} data from: ', input_file)
    def get_iter(reader):
        return track(
            reader,
            description='Loading rows...',
            disable=quiet,
            progress=progress,
        )
    # NOTE:
    #   csv モジュールの仕様上、ファイルは newline='' で開く必要がある。
    #   これを怠ると、引用符で囲まれたフィールド内の改行が壊れる。
    #   また、以前はファイルを開いたまま閉じていなかった。
    with open(input_file, 'r', encoding=encoding, newline='') as f:
        reader = csv.reader(f, delimiter=delimiter)
        if no_header:
            for i, row in enumerate(get_iter(reader)):
                assert isinstance(row, Iterable)
                d = OrderedDict()
                for j, field in enumerate(row):
                    d[f'{j}'] = field
                yield Row.from_dict(d)
        else:
            header: list[str] = []
            for i, row in enumerate(get_iter(reader)):
                assert isinstance(row, Iterable)
                row = list(row)
                if i == 0:
                    header = row
                    continue
                d = OrderedDict()
                for j, field in enumerate(row):
                    d[header[j]] = field
                yield Row.from_dict(d)

@register_loader('.csv')
def load_csv(
    input_file: str,
    progress: Progress | None = None,
    **kwargs,
) -> Generator[Row, None, None]:
    '''CSV ファイルを読み込む。'''
    yield from _load_delimited(
        input_file, ',', 'CSV', progress=progress, **kwargs,
    )

@register_loader('.tsv')
def load_tsv(
    input_file: str,
    progress: Progress | None = None,
    **kwargs,
) -> Generator[Row, None, None]:
    '''TSV ファイルを読み込む。'''
    yield from _load_delimited(
        input_file, '\t', 'TSV', progress=progress, **kwargs,
    )

@register_writer('.csv')
class CsvWriter(BaseWriter):
    # NOTE: csv モジュールの仕様上、書き込み先も newline='' で開く必要がある
    newline = ''
    delimiter = ','

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
        if not self.fobj:
            self._open()
            assert self.fobj is not None
        if self.writer is None:
            # NOTE: 最初の行を取得するまでヘッダーを決定できない
            self.writer = csv.DictWriter(
                self.fobj,
                fieldnames=row.flat.keys(),
                delimiter=self.delimiter,
            )
            self.writer.writeheader()
        self.writer.writerow(row.flat)

    def _write_all_rows(self):
        if self.rows:
            for row in self.rows:
                self._write_row(row)
        if self.fobj:
            self.fobj.close()

@register_writer('.tsv')
class TsvWriter(CsvWriter):
    delimiter = '\t'

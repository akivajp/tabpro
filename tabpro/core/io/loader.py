'''
Loader class is responsible for loading the data from the source.
'''

import os.path

from rich.console import Console

from . extensions.manage_loaders import get_loader
from ..classes.row import Row

from .. progress import (
    Progress,
)

# NOTE:
#   --sheet / --all-sheets は Excel 専用のオプション。
#   非 Excel 入力に対して指定された場合の警告を、CLI 実行全体で
#   1回だけにするためのフラグ (複数ファイル連結時の重複を避ける)。
_warned_sheet_option_ignored = False


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
        encoding: str | None = None,
        keep_rows: bool = False,
    ):
        self.source = source
        self.quiet = quiet
        self.no_header = no_header
        # NOTE:
        #   limit に 0 以下が指定されると全行を黙って読み飛ばし、
        #   空の出力ができてしまう (データ消失に見える) ため、
        #   ここで明示的なエラーにする。
        if limit is not None and limit < 1:
            raise ValueError(f'limit must be 1 or greater, got {limit}')
        self.limit = limit
        # NOTE: Excel 以外のローダーは **kwargs でこれらを読み捨てる
        self.sheet = sheet
        self.all_sheets = all_sheets
        # NOTE:
        #   テキスト系ローダー (CSV/TSV) 用の文字エンコーディング。
        #   None の場合は各ローダーの既定 (utf-8-sig) に任せる。
        self.encoding = encoding
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
        # NOTE:
        #   --sheet / --all-sheets は Excel 専用オプション。
        #   非 Excel 入力に対しては黙って読み捨てられていたため、
        #   タイポや指定間違いに気づけるように警告する。
        if (sheet or all_sheets) and self.extension.lower() not in ('.xls', '.xlsx'):
            self._warn_sheet_option_ignored()

    def _warn_sheet_option_ignored(self):
        '''非 Excel 入力に対する --sheet / --all-sheets の無視を警告する。'''
        global _warned_sheet_option_ignored
        if _warned_sheet_option_ignored or self.quiet:
            return
        _warned_sheet_option_ignored = True
        console = self.progress.console if self.progress else Console()
        option = '--all-sheets' if self.all_sheets else '--sheet'
        console.log(
            f'[yellow]warning: {option} applies to Excel files only '
            f'and was ignored for {self.source}[/yellow]'
        )

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
        # NOTE:
        #   limit はここでも判定する。JSONL や dbq のローダーは
        #   limit を受けて自前で止まるが、他の形式のローダーは
        #   受けても読み捨てるため、全形式で同じ挙動にするには
        #   Loader 側で打ち切る必要がある。
        num_loaded = 0
        # NOTE:
        #   encoding が指定された場合のみ下位のローダーに渡す。
        #   None をそのまま渡すと、下位の既定値 ('utf-8-sig') が
        #   None で上書きされてしまう。
        load_kwargs: dict = dict(
            quiet=self.quiet,
            no_header=self.no_header,
            progress=self.progress,
            limit=self.limit,
            sheet=self.sheet,
            all_sheets=self.all_sheets,
        )
        if self.encoding is not None:
            load_kwargs['encoding'] = self.encoding
        for row in self.fn_load(self.source, **load_kwargs):
            if self.limit is not None and num_loaded >= self.limit:
                break
            num_loaded += 1
            if rows is not None:
                rows.append(row)
            yield row
        if rows is not None:
            self.rows = rows

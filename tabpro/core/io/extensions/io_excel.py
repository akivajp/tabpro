from typing import (
    Generator,
)

import numpy as np
import pandas as pd

import openpyxl

from rich.console import Console

from .... logging import logger

from ... constants import SHEET_FIELD
from ... progress import Progress

from . manage_loaders import (
    Row,
    register_loader,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

def get_sheet_names(
    input_file: str,
) -> tuple[list[str], list[str]]:
    '''
    ブックに含まれるシート名と、そのうち可視のシート名を返す。

    Args:
        input_file: Excel ファイルのパス。

    Returns:
        (全シート名, 可視シート名) の組。
    '''
    if input_file.lower().endswith('.xlsx'):
        # NOTE: read_only にしないとブック全体をメモリに展開してしまう
        workbook = openpyxl.load_workbook(input_file, read_only=True)
        try:
            names = list(workbook.sheetnames)
            visible = [
                sheet.title for sheet in workbook.worksheets
                if sheet.sheet_state == 'visible'
            ]
        finally:
            workbook.close()
        return names, visible
    # NOTE:
    #   旧形式 (.xls) は openpyxl では開けないため pandas 経由で列挙する。
    #   シートの可視・不可視は取得できないため、全て可視として扱う。
    with pd.ExcelFile(input_file) as book:
        names = list(book.sheet_names)
    return names, names

def select_sheets(
    input_file: str,
    sheet: str | None,
    all_sheets: bool,
    console: Console,
    quiet: bool,
) -> list[str]:
    '''
    読み込む対象のシートを決定する。

    Args:
        input_file: Excel ファイルのパス。
        sheet: 明示的に指定されたシート名。
        all_sheets: 可視シートを全て読むかどうか。
        console: 警告の出力先。
        quiet: 警告を抑止するかどうか。

    Returns:
        読み込む対象のシート名のリスト。

    Raises:
        ValueError: 指定されたシートが存在しない、または可視シートが無い場合。
    '''
    names, visible = get_sheet_names(input_file)
    logger.debug('sheet names: %s', names)
    if sheet is not None:
        if sheet not in names:
            raise ValueError(
                f'Sheet not found: {sheet!r} in {input_file}. '
                f'Available sheets: {names}'
            )
        return [sheet]
    candidates = visible or names
    if not candidates:
        raise ValueError(f'No visible sheet found: {input_file}')
    if all_sheets:
        return candidates
    if len(candidates) > 1 and not quiet:
        # NOTE:
        #   以前は複数シートがあっても最初の1枚だけを黙って読んでいたため、
        #   残りのシートのデータが何の表示も無く欠落していた。
        console.log(
            f'[yellow]warning: {input_file} has {len(candidates)} sheets; '
            f'reading only {candidates[0]!r} and skipping {candidates[1:]}. '
            f'Use --sheet to choose one, or --all-sheets to read them all.'
            f'[/yellow]'
        )
    return [candidates[0]]

def load_workbook_rows(
    input_file: str,
    label: str,
    no_header: bool = False,
    console: Console | None = None,
    quiet: bool = False,
    progress: Progress | None = None,
    sheet: str | None = None,
    all_sheets: bool = False,
    **kwargs,
) -> Generator[Row, None, None]:
    '''
    Excel ブックの行を読み込む。

    Args:
        input_file: Excel ファイルのパス。
        label: ログ表示に用いる形式名。
        no_header: 先頭行をヘッダーとして扱わないかどうか。
        console: ログの出力先。
        quiet: ログと警告を抑止するかどうか。
        progress: 進捗表示に用いる Progress。
        sheet: 読み込むシート名。
        all_sheets: 可視シートを全て読むかどうか。

    Yields:
        1行分の Row。
    '''
    if console is None:
        console = progress.console if progress else Console()
    if not quiet:
        console.log(f'Loading {label} data from: ', input_file)
    target_sheets = select_sheets(
        input_file, sheet, all_sheets, console, quiet,
    )
    for sheet_name in target_sheets:
        if all_sheets and not quiet:
            console.log('reading sheet: ', sheet_name)
        # NOTE:
        #   Excel で勝手に日時データなどに変換されてしまうことを防ぐため、
        #   全ての値を文字列として読み込む。
        df = pd.read_excel(
            input_file,
            sheet_name=sheet_name,
            dtype=str,
            header=None if no_header else 0,
        )
        # NOTE: NaN を None に変換しておかないと厄介
        df = df.replace([np.nan], [None])
        for _, series in df.iterrows():
            row = Row.from_dict(series.to_dict())
            if all_sheets:
                # NOTE:
                #   複数シートをまとめて読む場合のみ、どのシート由来かを残す。
                #   単一シートの場合は出力を変えないため付与しない。
                row.staging[SHEET_FIELD] = sheet_name
            yield row

@register_loader('.xlsx')
def load_xlsx(
    input_file: str,
    **kwargs,
) -> Generator[Row, None, None]:
    '''Excel ブック (.xlsx) を読み込む。'''
    yield from load_workbook_rows(input_file, 'excel', **kwargs)

@register_loader('.xls')
def load_xls(
    input_file: str,
    **kwargs,
) -> Generator[Row, None, None]:
    '''
    旧形式の Excel ブック (.xls) を読み込む。

    書き出しには対応しない (旧形式を書ける現役のライブラリが存在しないため)。
    '''
    yield from load_workbook_rows(input_file, 'legacy excel', **kwargs)

@register_writer('.xlsx')
class ExcelWriter(BaseWriter):
    def __init__(
        self,
        target: str,
        **kwargs,
    ):
        super().__init__(target, **kwargs)

    def support_streaming(self):
        return False

    def _write_all_rows(
        self,
    ):
        # NOTE:
        #   空入力でも空のブックを出力する。
        #   以前は rows が空だと書き出し自体がスキップされ、
        #   出力ファイルが作られないまま終わっていた。
        df = pd.DataFrame([row.flat for row in self.rows])
        df.to_excel(self.target, index=False)
        self.finished = True

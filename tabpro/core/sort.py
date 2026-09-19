# -*- coding: utf-8 -*-

import os
import sys


from typing import (
    Any,
)

# 3-rd party modules

from . progress import Progress

# local

from .classes.row import Row

from . io import (
    check_writer,
    get_loader,
    get_writer,
)

from . console.views import (
    Panel,
)

from .functions.get_primary_key import get_primary_key

def make_sort_key(
    primary_key: tuple,
    numeric: bool,
):
    '''
    ソートに用いるキーを組み立てる。

    Args:
        primary_key: キー列の値の組。
        numeric: 数値として解釈できる値を数値順に並べるかどうか。
            False の場合は値をそのまま用いる (文字列は辞書順)。

    Returns:
        ソートに用いるキー。
    '''
    if not numeric:
        return primary_key
    parts = []
    for value in primary_key:
        if isinstance(value, (int, float)):
            parts.append((0, float(value)))
            continue
        try:
            parts.append((0, float(str(value).strip())))
        except (TypeError, ValueError):
            # NOTE:
            #   数値として解釈できない値は数値の後ろに、
            #   文字列としての順序で並べる。
            #   グループ番号 (0 / 1) を先頭に置くことで、
            #   数値と文字列が混ざっても要素同士の比較が型不一致にならない。
            parts.append((1, str(value)))
    return tuple(parts)

def sort(
    sort_keys: list[str] | str,
    input_files: list[str],
    output_file: str | None = None,
    reverse: bool = False,
    numeric: bool = False,
):
    progress = Progress(
        redirect_stdout = False,
    )
    progress.start()
    console = progress.console
    console.log('input_files: ', input_files)
    console.log('numeric: ', numeric)
    if output_file:
        check_writer(output_file)
    all_input_row_items: list[tuple[Any, Row]] = []
    for input_file in input_files:
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        loader = get_loader(
            input_file,
            progress=progress,
        )
        for index, row in enumerate(loader):
            primary_key = get_primary_key(row, sort_keys)
            all_input_row_items.append(
                (make_sort_key(primary_key, numeric), row)
            )
    if not all_input_row_items:
        raise ValueError(f'no rows to sort in: {input_files}')
    console.log('# input rows: ', len(all_input_row_items))
    console.log('sorting rows...')
    try:
        all_input_row_items.sort(
            key=lambda x: x[0],
            reverse=reverse,
        )
    except TypeError:
        # NOTE:
        #   JSON 由来のデータではキーに数値と文字列が混ざりうるため、
        #   混在時に TypeError で停止する代わりに文字列に揃えて並べる。
        all_input_row_items.sort(
            key=lambda x: tuple(str(item) for item in x[0]),
            reverse=reverse,
        )
    if output_file is None and sys.stdout.isatty():
        console.print(Panel(
            all_input_row_items[0][1],
            title='first row',
            title_align='left',
            border_style='cyan',
        ))
    elif output_file:
        writer = get_writer(
            output_file,
            progress=progress,
        )
        for key, row in all_input_row_items:
            writer.push_row(row)
        writer.close()
    progress.stop()

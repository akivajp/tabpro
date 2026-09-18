import os.path
import pandas as pd

from typing import Callable, TypeAlias

from rich.console import Console

from ... progress import Progress

from ..writer import BaseWriter

# NOTE:
#   PEP 695 の `type` 文は Python 3.12 以降でしか使えないため、
#   3.10 以降をサポートするために TypeAlias による定義を用いる。
Saver: TypeAlias = Callable[[pd.DataFrame, str], None]

dict_writers: dict[str, type[BaseWriter]] = {}

from ...classes.row import Row

def register_writer(
    ext: str,
):
    def decorator(writer: type[BaseWriter]):
        dict_writers[ext] = writer
        return writer
    return decorator

def check_writer(
    output_file: str,
):
    ext = os.path.splitext(output_file)[1]
    if ext not in dict_writers:
        # NOTE:
        #   読み込めるのに書き出せない形式 (.xls など) は、
        #   単に未対応と言われるより理由が分かるほうがよい。
        from . manage_loaders import dict_loaders
        if ext in dict_loaders:
            raise ValueError(
                f'{ext} files can be read but not written. '
                f'Supported output types: {sorted(dict_writers)}'
            )
        raise ValueError(f'Unsupported file type: {ext}')
    writer_class = dict_writers[ext]
    return writer_class

def get_writer(
    output_file: str,
    progress: Progress | None = None,
) -> BaseWriter:
    writer_class = check_writer(output_file)
    return writer_class(
        output_file,
        progress=progress,
    )

def save(
    rows: list[Row],
    output_file: str,
    progress: Progress | None = None,
):
    writer = get_writer(output_file, progress=progress)
    writer.push_rows(rows)
    writer.close()

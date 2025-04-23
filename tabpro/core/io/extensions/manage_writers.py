import os.path
import pandas as pd

from typing import Callable

from rich.console import Console

from ... progress import Progress

from ..writer import BaseWriter

type Saver = Callable[[pd.DataFrame, str], None]

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

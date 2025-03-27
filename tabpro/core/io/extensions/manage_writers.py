import os.path
import pandas as pd

from typing import Callable

from rich.console import Console

from ... progress import Progress

from ..writer import BaseWriter

type Saver = Callable[[pd.DataFrame, str], None]

dict_writers: dict[str, BaseWriter] = {}

def register_writer(
    ext: str,
):
    def decorator(writer: BaseWriter):
        dict_writers[ext] = writer
        return writer
    return decorator

def get_writer(
    output_file: str,
    progress: Progress | None = None,
):
    ext = os.path.splitext(output_file)[1]
    if ext not in dict_writers:
        raise ValueError(f'Unsupported file type: {ext}')
    writer_class = dict_writers[ext]
    return writer_class(
        output_file,
        progress=progress,
    )

def save(
    rows: list[dict],
    output_file: str,
):
    writer = get_writer(output_file)
    writer.push_rows(rows)
    writer.close()

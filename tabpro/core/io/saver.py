import os.path
import pandas as pd

from typing import Callable

type Saver = Callable[[pd.DataFrame, str], None]

dict_savers: dict[str, Saver] = {}
def register_saver(
    ext: str,
):
    def decorator(saver):
        dict_savers[ext] = saver
        return saver
    return decorator

def get_saver(
    output_file: str,
):
    ext = os.path.splitext(output_file)[1]
    if ext not in dict_savers:
        raise ValueError(f'Unsupported file type: {ext}')
    saver = dict_savers[ext]
    return saver

def save(
    df: pd.DataFrame,
    output_file: str,
):
    saver = get_saver(output_file)
    saver(df, output_file)

from icecream import ic
from tqdm.auto import tqdm
import numpy as np
import pandas as pd

from rich.console import Console

from logzero import logger

from . manage_loaders import (
    Row,
    register_loader,
)
from . manage_writers import (
    BaseWriter,
    register_writer,
)

@register_loader('.xlsx')
def load_excel(
    input_file: str,
    no_header: bool = False,
    console: Console | None = None,
    quiet: bool = False,
    **kwargs,
):
    if not quiet:
        if console is None:
            console = Console()
        console.log('Loading excel data from: ', input_file)
    # NOTE: Excelで勝手に日時データなどに変換されてしまうことを防ぐため
    dtype = str
    if no_header:
        df = pd.read_excel(
            input_file,
            header=None,
            dtype=dtype
        )
    else:
        df = pd.read_excel(
            input_file,
            dtype=str
        )
    # NOTE: NaN を None に変換しておかないと厄介
    df = df.replace([np.nan], [None])
    #return df
    for i, row in df.iterrows():
        yield Row.from_dict(row.to_dict())

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
        df = pd.DataFrame([row.flat for row in self.rows])
        df.to_excel(self.target, index=False)
        self.finished = True

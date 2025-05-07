from icecream import ic
import numpy as np
import pandas as pd

import openpyxl

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
    # シートの選択
    wb = openpyxl.load_workbook(input_file)
    sheet_names = wb.sheetnames
    logger.debug(f'Sheet names: {sheet_names}')
    target_sheet_name = None
    if len(sheet_names) == 1:
        target_sheet_name = sheet_names[0]
    else:
        for sheet in wb.worksheets:
            if sheet.sheet_state == 'visible':
                target_sheet_name = sheet.title
                break
    if target_sheet_name is None:
        raise ValueError('No visible sheet found')
    # NOTE:
    #   Excelで勝手に日時データなどに変換されてしまうことを防ぐため
    #   (To prevent Excel from automatically converting data like dates and times)
    dtype = str
    if no_header:
        df = pd.read_excel(
            input_file,
            header=None,
            dtype=dtype,
            sheet_name=target_sheet_name,
        )
    else:
        df = pd.read_excel(
            input_file,
            dtype=str,
            sheet_name=target_sheet_name,
        )
    # NOTE:
    #   NaN を None に変換しておかないと厄介
    #   (Need to convert NaN to None to avoid complications)
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

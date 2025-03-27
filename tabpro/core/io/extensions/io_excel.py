from icecream import ic
from tqdm.auto import tqdm
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
    **kwargs,
):
    quiet = kwargs.get('quiet', False)
    if not quiet:
        logger.info('Loading Excel data from: %s', input_file)
    # NOTE: Excelで勝手に日時データなどに変換されてしまうことを防ぐため
    df = pd.read_excel(input_file, dtype=str)
    # NOTE: 列番号でもアクセスできるようフィールドを追加する
    df_with_column_number = pd.read_excel(
        input_file, dtype=str, header=None, skiprows=1
    )
    new_column_names = [f'__values__.{i}' for i in df_with_column_number.columns]
    df2 = df_with_column_number.rename(columns=dict(
        zip(df_with_column_number.columns, new_column_names)
    ))
    df = pd.concat([df, df2], axis=1)
    df = df.dropna(axis=0, how='all')
    df = df.dropna(axis=1, how='all')
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
        if not self.quiet:
            #logger.info('Writing Excel data into: %s', self.output_file)
            console = self._get_console()
            console.log('Writing excel data into: ', self.target)
        df = pd.DataFrame([row.flat for row in self.rows])
        df.to_excel(self.target, index=False)
        self.finished = True

    def close(
        self,
    ):
        if self.finished: return
        self._write_all_rows()

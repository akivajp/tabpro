import pandas as pd

from . loader import register_loader
from . saver import register_saver

@register_loader('.csv')
def load_csv(
    input_file: str,
    **kwargs,
):
    skip_header = kwargs.get('skip_header', False)
    # utf-8
    #df = pd.read_csv(input_file)
    # UTF-8 with BOM
    if skip_header:
        df = pd.read_csv(
            input_file,
            encoding='utf-8-sig',
            header=None,
        )
        #new_column_names = [f'__values__.{i}' for i in df.columns]
        new_column_names = [f'{i}' for i in df.columns]
        df = df.rename(columns=dict(
            zip(df.columns, new_column_names)
        ))
    else:
        df = pd.read_csv(
            input_file,
            encoding='utf-8-sig',
        )
    return df

@register_saver('.csv')
def save_csv(
    df: pd.DataFrame,
    output_file: str,
):
    # utf-8
    #df.to_csv(output_file, index=False)
    # UTF-8 with BOM
    df.to_csv(output_file, index=False, encoding='utf-8-sig')

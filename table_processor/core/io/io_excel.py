import pandas as pd

from . loader import register_loader
from . saver import register_saver

@register_loader('.xlsx')
def load_excel(
    input_file: str,
    **kwargs,
):
    #df = pd.read_excel(input_file)
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
    return df

@register_saver('.xlsx')
def save_excel(
    df: pd.DataFrame,
    output_file: str,
):
    # openpyxl
    df.to_excel(output_file, index=False)
    # xlsxwriter
    #writer = pd.ExcelWriter(
    #    output_file,
    #    engine='xlsxwriter',
    #    engine_kwargs={
    #        'options': {
    #            'strings_to_urls': False,
    #        },
    #    }
    #)
    #df.to_excel(writer, index=False)
    #writer.close()

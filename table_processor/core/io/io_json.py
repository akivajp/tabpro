import json
import pandas as pd

from . loader import register_loader
from . saver import register_saver

from .. functions.flatten_row import flatten_row
from .. functions.nest_row import nest_row

@register_loader('.json')
def load_json(
    input_file: str,
    **kiwargs,
):
    with open(input_file, 'r') as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f'Invalid JSON array data: {input_file}')
    #ic(data[0])
    rows = []
    for row in data:
        new_row = flatten_row(row)
        rows.append(new_row)
    df = pd.DataFrame(rows)
    return df

@register_saver('.json')
def save_json(
    df: pd.DataFrame,
    output_file: str,
):
    # NOTE: この方法だとスラッシュがすべてエスケープされてしまった
    #df.to_json(
    #    output_file,
    #    orient='records',
    #    force_ascii=False,
    #    indent=2,
    #    escape_forward_slashes=False,
    #)
    #ic(df.iloc[0])
    data = df.to_dict(orient='records')
    #ic(data[0])
    data = [nest_row(row) for row in data]
    #ic(data[0])
    with open(output_file, 'w') as f:
        json.dump(
            data,
            f,
            indent=2,
            ensure_ascii=False,
        )

@register_loader('.jsonl')
def load_jsonl(
    input_file: str,
    **kwargs,
):
    rows = []
    with open(input_file, 'r') as f:
        for line in f:
            row = json.loads(line)
            rows.append(row)
    df = pd.DataFrame(rows)
    return df

@register_saver('.jsonl')
def save_jsonl(
    df: pd.DataFrame,
    output_file: str,
):
    # NOTE: この方法だとスラッシュがすべてエスケープされてしまった
    #df.to_json(
    #    output_file,
    #    orient='records',
    #    lines=True,
    #    force_ascii=False,
    #)
    with open(output_file, 'w') as f:
        for index, row in df.iterrows():
            data = row.to_dict()
            json.dump(
                data,
                f,
                ensure_ascii=False,
            )
            f.write('\n')
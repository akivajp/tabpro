# -*- coding: utf-8 -*-

import json
import os

import pandas as pd
from icecream import ic

map_loaders: dict[str, callable] = {}
def register_loader(
    ext: str,
):
    def decorator(loader):
        map_loaders[ext] = loader
        return loader
    return decorator

map_savers: dict[str, callable] = {}
def register_saver(
    ext: str,
):
    def decorator(saver):
        map_savers[ext] = saver
        return saver
    return decorator

@register_loader('.xlsx')
def load_excel(
    input_file: str,
):
    df = pd.read_excel(input_file)
    return df

@register_saver('.json')
def save_json(
    df: pd.DataFrame,
    output_file: str,
):
    df.to_json(
        output_file,
        orient='records',
        force_ascii=False,
        indent=2,
    )

@register_saver('.jsonl')
def save_jsonl(
    df: pd.DataFrame,
    output_file: str,
):
    df.to_json(
        output_file,
        orient='records',
        lines=True,
        force_ascii=False,
    )

def convert(
    input_files: list[str],
    output_file: str | None = None,
):
    ic()
    ic(input_files)
    df_list = []
    if output_file:
        ext = os.path.splitext(output_file)[1]
        if ext not in map_savers:
            raise ValueError(f'Unsupported file type: {ext}')
        saver = map_savers[ext]
    for input_file in input_files:
        ic(input_file)
        if not os.path.exists(input_file):
            raise FileNotFoundError(f'File not found: {input_file}')
        ext = os.path.splitext(input_file)[1]
        ic(ext)
        if ext not in map_loaders:
            raise ValueError(f'Unsupported file type: {ext}')
        df = map_loaders[ext](input_file)
        #ic(df)
        ic(len(df))
        ic(df.columns)
        ic(df.iloc[0])
        df_list.append(df)
    all_df = pd.concat(df_list)
    #ic(all_df)
    ic(len(all_df))
    ic(all_df.columns)
    ic(all_df.iloc[0])
    if output_file:
        ic('Saing to: ', output_file)
        saver(all_df, output_file)

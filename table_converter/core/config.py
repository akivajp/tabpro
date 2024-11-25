# -*- coding: utf-8 -*-

from collections import OrderedDict
import dataclasses
from typing import Mapping

from icecream import ic
import yaml

type FlatFieldMap = Mapping[str, str]
type FieldMap = Mapping[str, str|FieldMap]

@dataclasses.dataclass
class ProcessConfig:
    split_by_newline: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)

@dataclasses.dataclass
class Config:
    map: FieldMap = dataclasses.field(default_factory=OrderedDict)
    process: ProcessConfig = dataclasses.field(default_factory=ProcessConfig)

def flatten(
    mapping: FieldMap,
    parent_key: str = '',
    new_mapping: FlatFieldMap | None = None,
) -> FlatFieldMap:
    if new_mapping is None:
        new_mapping = OrderedDict()
    for key, mapped in mapping.items():
        new_key = f'{parent_key}.{key}' if parent_key else key
        if isinstance(mapped, Mapping):
            flatten(mapped, new_key, new_mapping)
        else:
            new_mapping[new_key] = mapped
    return new_mapping

def setup_config(
    config_path: str | None = None,
):
    config = Config()
    if config_path:
        if config_path.endswith('.yaml'):
            yaml.add_constructor(
                yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                lambda loader, node: OrderedDict(loader.construct_pairs(node)),
            )
            with open(config_path, 'r') as f:
                loaded = yaml.load(f, yaml.Loader)
        else:
            raise ValueError(
                'Only YAML configuration files are supported.'
            )
        ic(loaded)
        if 'map' in loaded:
            config.map = flatten(loaded['map'])
        if 'process' in loaded:
            if 'split_by_newline' in loaded['process']:
                config.process.split_by_newline = flatten(loaded['process']['split_by_newline'])
    return config

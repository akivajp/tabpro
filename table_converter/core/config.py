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
    assign_constants: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    assign_formats: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    split_by_newline: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)

    def __setitem__(self, key, value):
        setattr(self, key, value)

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
        dict_process = loaded.get('process')
        if isinstance(dict_process, Mapping):
            for process_key in [
                'assign_constants',
                'assign_formats',
                'split_by_newline',
            ]:
                dict_subprocess = dict_process.get(process_key)
                if isinstance(dict_subprocess, Mapping):
                    config.process[process_key] = flatten(loaded['process'][process_key])
    return config

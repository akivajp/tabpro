# -*- coding: utf-8 -*-

from collections import OrderedDict
import dataclasses
from typing import (
    Literal,
    Mapping,
)

from icecream import ic
import yaml

from . functions.flatten import (
    FieldMap,
    FlatFieldMap,
    flatten,
)

@dataclasses.dataclass
class AssignIdConfig:
    primary: list[str]
    context: list[str] | None = None

@dataclasses.dataclass
class FilterConfig:
    field: str
    operator: Literal['==', '!=', '>', '>=', '<', '<=']
    value: str

@dataclasses.dataclass
class ProcessConfig:
    assign_constants: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    assign_formats: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    assign_ids: Mapping[str, AssignIdConfig] = dataclasses.field(default_factory=OrderedDict)
    #filter_eq: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    filter: list[FilterConfig] = dataclasses.field(default_factory=list)
    omit_fields: list[str] = dataclasses.field(default_factory=list)
    split_by_newline: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)

    def __setitem__(self, key, value):
        setattr(self, key, value)

@dataclasses.dataclass
class Config:
    map: FieldMap = dataclasses.field(default_factory=OrderedDict)
    process: ProcessConfig = dataclasses.field(default_factory=ProcessConfig)

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
        setup_process_config(config, loaded)
    return config

def setup_process_config(
    config: Config,
    loaded: Mapping,
):
    dict_process = loaded.get('process')
    if isinstance(dict_process, Mapping):
        for process_key in [
            'assign_constants',
            'assign_formats',
            'filter_eq',
            'split_by_newline',
        ]:
            dict_subprocess = dict_process.get(process_key)
            if isinstance(dict_subprocess, Mapping):
                config.process[process_key] = flatten(loaded['process'][process_key])
        setup_process_assign_ids_config(config, dict_process)

def setup_process_assign_ids_config(
    config: Config,
    dict_process: Mapping,
):
    dict_subprocess = dict_process.get('assign_ids')
    if isinstance(dict_subprocess, Mapping):
        for key, value in dict_subprocess.items():
            if isinstance(value, Mapping):
                primary = value.get('primary')
                if not primary:
                    ic.enable()
                    ic(value)
                    ic(value.get('primary'))
                    raise ValueError(
                        'Primary field is required for assign_ids.'
                    )
                config.process.assign_ids[key] = AssignIdConfig(
                    primary = value.get('primary', []),
                    #given = value.get('given', None),
                    context = value.get('context', None),
                )
            elif isinstance(value, list):
                config.process.assign_ids[key] = AssignIdConfig(
                    primary = value,
                )
            elif isinstance(value, str):
                config.process.assign_ids[key] = AssignIdConfig(
                    primary = [value],
                )
            else:
                ic.enable()
                ic(value)
                ic(type(value))
                raise ValueError(
                    f'Unsupported assign_ids value type: {type(value)}'
                )

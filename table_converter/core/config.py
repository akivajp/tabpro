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
    operator: Literal['==', '!=', '>', '>=', '<', '<=', 'not-in']
    #value: str
    value: str | list[str]

@dataclasses.dataclass
class SplitConfig:
    field: str
    delimiter: str

@dataclasses.dataclass
class ProcessConfig:
    assign_constants: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    assign_formats: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    assign_ids: Mapping[str, AssignIdConfig] = dataclasses.field(default_factory=OrderedDict)
    #filter_eq: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    filter: list[FilterConfig] = dataclasses.field(default_factory=list)
    omit_fields: list[str] = dataclasses.field(default_factory=list)
    #split_by_newline: FlatFieldMap = dataclasses.field(default_factory=OrderedDict)
    split: Mapping[str, SplitConfig] = dataclasses.field(default_factory=OrderedDict)

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
        ]:
            dict_subprocess = dict_process.get(process_key)
            if isinstance(dict_subprocess, Mapping):
                config.process[process_key] = flatten(loaded['process'][process_key])
        setup_process_assign_ids_config(config, dict_process)
        setup_process_filter_config(config, dict_process)
        setup_process_split_config(config, dict_process)

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

def setup_process_filter_config(
    config: Config,
    dict_process: Mapping,
):
    list_subprocess = dict_process.get('filter')
    if list_subprocess is None:
        return
    if not isinstance(list_subprocess, list):
        raise ValueError(
            'Filter must be a list.'
        )
    for item in list_subprocess:
        if isinstance(item, Mapping):
            field = item.get('field')
            if not field:
                ic.enable()
                ic(item)
                ic(item.get('field'))
                raise ValueError(
                    'Field is required for filter.'
                )
            operator = item.get('operator')
            if not operator:
                ic.enable()
                ic(item)
                ic(item.get('operator'))
                raise ValueError(
                    'Operator is required for filter.'
                )
            value = item.get('value')
            if not value:
                ic.enable()
                ic(item)
                ic(item.get('value'))
                raise ValueError(
                    'Value is required for filter.'
                )
            config.process.filter.append(FilterConfig(
                field = field,
                operator = operator,
                value = value,
            ))
        else:
            ic.enable()
            ic(item)
            ic(type(item))
            raise ValueError(
                f'Unsupported filter item type: {type(item)}'
            )

def setup_process_split_config(
    config: Config,
    dict_process: Mapping,
):
    dict_subprocess = dict_process.get('split')
    if isinstance(dict_subprocess, Mapping):
        for key, value in dict_subprocess.items():
            if isinstance(value, Mapping):
                field = value.get('field')
                if not field:
                    ic.enable()
                    ic(value)
                    ic(value.get('field'))
                    raise ValueError(
                        'Field is required for split.'
                    )
                delimiter = value.get('delimiter')
                if not delimiter:
                    ic.enable()
                    ic(value)
                    ic(value.get('delimiter'))
                    raise ValueError(
                        'Delimiter is required for split.'
                    )
                config.process.split[key] = SplitConfig(
                    field = field,
                    delimiter = delimiter,
                )
            else:
                ic.enable()
                ic(value)
                ic(type(value))
                raise ValueError(
                    f'Unsupported assign_ids value type: {type(value)}'
                )

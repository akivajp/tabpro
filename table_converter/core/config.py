# -*- coding: utf-8 -*-

from collections import OrderedDict
import dataclasses
from typing import Mapping

from icecream import ic
import yaml

type FlatFieldMap = Mapping[str, str]
type FieldMap = Mapping[str, str|FieldMap]

@dataclasses.dataclass
class Config:
    #map: OrderedDict|None = dataclasses.field(default_factory=lambda: None)
    map: FieldMap|None = dataclasses.field(default_factory=lambda: None)

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
    if config_path:
        if config_path.endswith('.yaml'):
            yaml.add_constructor(
                yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
                lambda loader, node: OrderedDict(loader.construct_pairs(node)),
            )
            with open(config_path, 'r') as f:
                data = yaml.load(f, yaml.Loader)
                #ic(data)
                config = Config(
                    #**data,
                    map = flatten(data.get('map', {})),
                )
        else:
            raise ValueError(
                'Only YAML configuration files are supported.'
            )
    else:
        config = Config(
        )
    return config

# -*- coding: utf-8 -*-

from collections import OrderedDict
import dataclasses
from typing import (
    Any,
    Literal,
    Mapping,
)

from icecream import ic
import yaml

from . functions.flatten_row import (
    FlatFieldMap,
    flatten_row,
)

from . types import (
    ActionConfig,
    AssignFormatConfig,
    AssignIdConfig,
    AssignConstantConfig,
    FilterConfig,
    SplitConfig,
    PickConfig,
)

@dataclasses.dataclass
class AssignArrayConfig:
    field: str
    optional: bool = True

@dataclasses.dataclass
class ProcessConfig:
    assign_array: Mapping[str, list[AssignArrayConfig]] = dataclasses.field(default_factory=OrderedDict)

    def __setitem__(self, key, value):
        setattr(self, key, value)

@dataclasses.dataclass
class Config:
    actions: list[ActionConfig] = dataclasses.field(default_factory=list)

    pick: list[PickConfig] = dataclasses.field(default_factory=list)
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
        if 'pick' in loaded:
            if not isinstance(loaded['pick'], Mapping | list):
                raise ValueError(
                    f'pick must be a dict or list, not {type(loaded["pick"])}'
                )
            if isinstance(loaded['pick'], Mapping):
                for key, value in flatten_row(loaded['pick']).items():
                    config.pick.append(PickConfig(
                        target = key,
                        source = value,
                    ))
            if isinstance(loaded['pick'], list):
                for item in loaded['pick']:
                    if not isinstance(item, str):
                        raise ValueError(
                            'Pickup list must contain strings.'
                        )
                    config.pick.append(PickConfig(
                        target = item,
                        source = item,
                    ))
        setup_process_config(config, loaded)
    return config

def setup_process_config(
    config: Config,
    loaded: Mapping,
):
    dict_process = loaded.get('process')
    if isinstance(dict_process, Mapping):
        for process_key in [
            'assign_length',
        ]:
            dict_subprocess = dict_process.get(process_key)
            if isinstance(dict_subprocess, Mapping):
                config.process[process_key] = flatten_row(loaded['process'][process_key])
        for process_key in [
            'assign_constants',
            'assign_formats',
        ]:
            dict_subprocess = dict_process.get(process_key)
            if isinstance(dict_subprocess, Mapping):
                for key, value in dict_subprocess.items():
                    if process_key == 'assign_constants':
                        config.actions.append(AssignConstantConfig(
                            target = key,
                            value = value,
                        ))
                    if process_key == 'assign_formats':
                        config.actions.append(AssignFormatConfig(
                            target = key,
                            format = value,
                        ))
        setup_process_assign_ids_config(config, dict_process)
        setup_process_assign_array_config(config, dict_process)
        setup_process_filter_config(config, dict_process)
        setup_process_push_config(config, dict_process)
        setup_process_split_config(config, dict_process)

def raise_error_for_unsupported_type(
    value: Any,
    should_be: str | None = None,
):
    ic.enable()
    ic()
    ic(value)
    ic(type(value))
    if should_be:
        raise ValueError(
            f'Unsupported value type: {type(value)}. Should be {should_be}.'
        )
    else:
        raise ValueError(
            f'Unsupported value type: {type(value)}'
        )

def require_item(
    mapping: Mapping,
    key: str,
    str_for: str
):
    if key not in mapping:
        raise ValueError(
            f'{key} is required for {str_for}.'
        )
    return mapping[key]

def setup_process_assign_ids_config(
    config: Config,
    dict_process: Mapping,
):
    dict_subprocess = dict_process.get('assign_ids')
    str_for = 'assign_ids'
    if isinstance(dict_subprocess, Mapping):
        for key, value in dict_subprocess.items():
            if isinstance(value, Mapping):
                primary = require_item(value, 'primary', str_for)
                context = value.get('context', None)
                if isinstance(primary, str):
                    primary = [primary]
                if isinstance(context, str):
                    context = [context]
                config.actions.append(AssignIdConfig(
                    target = key,
                    primary = primary,
                    context = context,
                ))
            elif isinstance(value, list):
                config.actions.append(AssignIdConfig(
                    target = key,
                    primary = value,
                ))
            elif isinstance(value, str):
                config.actions.append(AssignIdConfig(
                    target = key,
                    primary = [value],
                ))
            else:
                raise_error_for_unsupported_type(value, 'dict, list, or str')

def setup_process_assign_array_config(
    config: Config,
    dict_process: Mapping,
):
    dict_subprocess = dict_process.get('assign_array')
    if dict_subprocess is None:
        return
    if not isinstance(dict_subprocess, Mapping):
        raise ValueError(
            'Assign array must be a dictionary.'
        )
    for key, value in dict_subprocess.items():
        if isinstance(value, list):
            config.process.assign_array[key] = []
            array = value
            for item in array:
                if isinstance(item, Mapping):
                    field = item.get('field')
                    optional = item.get('optional', False)
                    if field is None:
                        ic.enable()
                        ic(item)
                        ic(item.get('field'))
                        raise ValueError(
                            'Field is required for assign_array.'
                        )
                    config.process.assign_array[key].append(AssignArrayConfig(
                        field = field,
                        optional = optional,
                    ))
                elif isinstance(item, str):
                    config.process.assign_array[key].append(AssignArrayConfig(
                        field = item,
                        optional = True,
                    ))
                else:
                    raise_error_for_unsupported_type(item, 'dict or str')
        else:
            raise_error_for_unsupported_type(value, 'list')

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
            config.actions.append(FilterConfig(
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
        
def setup_process_push_config(
    config: Config,
    dict_process: Mapping,
):
    list_subprocess = dict_process.get('push')
    if list_subprocess is None:
        return
    if not isinstance(list_subprocess, list):
        raise ValueError(
            'push must be a list.'
        )
    str_for = 'push'
    for item in list_subprocess:
        if isinstance(item, Mapping):
            target = require_item(item, 'target', str_for)
            source = require_item(item, 'source', str_for)
            condition = item.get('condition')
            config.process.push.append(PushConfig(
                target = target,
                source = source,
                condition = condition,
            ))
        else:
            raise_error_for_unsupported_type(item, 'dict')

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
                #config.process.split[key] = SplitConfig(
                #    field = field,
                #    delimiter = delimiter,
                #)
                config.actions.append(SplitConfig(
                    target = key,
                    source = field,
                    delimiter = delimiter,
                ))
            else:
                ic.enable()
                ic(value)
                ic(type(value))
                raise ValueError(
                    f'Unsupported assign_ids value type: {type(value)}'
                )

def setup_pick_with_args(
    config: Config,
    list_fields: list[str],
):
    ic(list_fields)
    for field in list_fields:
        if '=' in field:
            target, source = field.split('=')
            config.pick.append(PickConfig(
                target = target.strip(),
                source = source.strip(),
            ))
        else:
            config.pick.append(PickConfig(
                target = field.strip(),
                source = field.strip(),
            ))

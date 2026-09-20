# -*- coding: utf-8 -*-

from collections import OrderedDict
import dataclasses
from typing import (
    Any,
    Mapping,
)

from rich.console import Console

import yaml

from . functions.flatten_row import (
    flatten_row,
)

from .actions.types import (
    BaseActionConfig,
    AssignArrayConfig,
    AssignArrayElementConfig,
    AssignFormatConfig,
    AssignIdConfig,
    AssignLengthConfig,
    AssignConstantConfig,
    FilterConfig,
    SplitConfig,
    PickConfig,
    PushConfig,
)

@dataclasses.dataclass
class Config:
    actions: list[BaseActionConfig] = dataclasses.field(default_factory=list)
    pick: list[PickConfig] = dataclasses.field(default_factory=list)

class ConfigLoader(yaml.Loader):
    '''設定ファイル読み込み専用の Loader。

    NOTE:
        yaml.add_constructor を既定の yaml.Loader に対して呼ぶと
        プロセス全体の Loader が書き換わるため、専用サブクラスに閉じ込める。
    '''

def construct_mapping_detecting_duplicates(
    loader: ConfigLoader,
    node,
):
    '''
    マッピングを OrderedDict で読み込みつつ、重複したキーを記録する。

    NOTE:
        YAML 標準では重複キーは「後勝ち」で黙って前者が捨てられる。
        コピペミスやマージミスで「書いたはずの設定が消える」事故の
        温床になるため、重複したキーを loader に記録し、後で
        warn_duplicate_config_keys が警告に使う。値の解釈 (後勝ち)
        自体は YAML 標準どおり変更しない。
    '''
    seen: set = set()
    pairs = []
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if key in seen:
            recorded = getattr(loader, 'duplicate_keys', None)
            if recorded is None:
                recorded = loader.duplicate_keys = []
            recorded.append(key)
        seen.add(key)
        value = loader.construct_object(value_node, deep=True)
        pairs.append((key, value))
    return OrderedDict(pairs)

# NOTE:
#   マッピングのキー順を保持するため、OrderedDict で読み込む。
#   登録は専用 Loader に対してモジュール読み込み時に1回だけ行う。
yaml.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    construct_mapping_detecting_duplicates,
    Loader=ConfigLoader,
)

# NOTE:
#   設定ファイルでサポートされるトップレベルキーと process 配下のキー。
#   ここに無いキーは以前は黙って無視されており、process: を proces: と
#   書き間違えても何の表示も無く設定が適用されないままになっていた。
KNOWN_CONFIG_KEYS = ['pick', 'process']

KNOWN_PROCESS_KEYS = [
    'assign_length',
    'assign_constants',
    'assign_formats',
    'assign_ids',
    'assign_array',
    'filter',
    'push',
    'split',
]

# NOTE:
#   キー名は正しいのに値の型が違う場合 (例: assign_constants に
#   文字列を書く) は、以前は isinstance チェックを黙って通過して
#   スキップされ、設定したつもりの処理が反映されないまま納品物が
#   できていた。ここに挙げたキーは想定型と違うと警告する。
#   filter / push / assign_array は既存の実装が ValueError を出す
#   ため、この表には含めない (エラーのまま、挙動不変)。
EXPECTED_PROCESS_VALUE_TYPES = {
    'assign_length': Mapping,
    'assign_constants': Mapping,
    'assign_formats': Mapping,
    'assign_ids': Mapping,
    'split': Mapping,
}

def warn_wrong_typed_process_keys(
    dict_process: Mapping,
    console: Console | None = None,
    no_warnings: bool = False,
):
    '''
    process 配下で値の型が想定と違うキーを警告する。

    NOTE:
        エラーにはしない (既存の挙動はスキップのまま不変)。
        未知キー警告と同じく --no-warnings で抑制できる。

    Args:
        dict_process: 設定ファイルの process 配下の Mapping。
        console: 警告の出力先。
        no_warnings: 警告を抑止するかどうか (--no-warnings 対応)。
    '''
    if console is None or no_warnings:
        return
    for key, value in dict_process.items():
        expected = EXPECTED_PROCESS_VALUE_TYPES.get(str(key))
        if expected is None or isinstance(value, expected):
            continue
        console.log(
            f'[yellow]warning: process.{key} of the config file should be '
            f'a {expected.__name__}, got {type(value).__name__} '
            f'({value!r}), and it was ignored.[/yellow]'
        )

def warn_duplicate_config_keys(
    duplicate_keys: list,
    where: str,
    console: Console | None = None,
    no_warnings: bool = False,
):
    '''
    YAML の重複キーを警告する (値の解釈は YAML 標準どおり後勝ち)。

    Args:
        duplicate_keys: 検出された重複キーの一覧。
        where: 警告メッセージに用いる対象の説明 (ファイルパスなど)。
        console: 警告の出力先。
        no_warnings: 警告を抑止するかどうか (--no-warnings 対応)。
    '''
    if not duplicate_keys or console is None or no_warnings:
        return
    console.log(
        f'[yellow]warning: {where} contains duplicate key(s) '
        f'{[str(key) for key in duplicate_keys]}; '
        f'the last value wins.[/yellow]'
    )

def setup_config(
    config_path: str | None = None,
    console: Console | None = None,
    no_warnings: bool = False,
):
    config = Config()
    if config_path:
        # NOTE: validate コマンドと同様に .yaml / .yml の両方を受け付ける
        if config_path.endswith(('.yaml', '.yml')):
            # NOTE: ロケールに依存しないよう、エンコーディングを明示する
            with open(config_path, 'r', encoding='utf-8') as f:
                # NOTE:
                #   重複キーの検出結果を loader から受け取るため、
                #   yaml.load() の内部で隠されるのではなく、
                #   loader を明示的に組み立てる。
                loader = ConfigLoader(f)
                loaded = loader.get_single_data()
            # NOTE:
            #   空ファイル (None) やリスト・文字列など Mapping 以外の
            #   内容は、後続の .get() での TypeError の代わりに
            #   ここで分かりやすいエラーにする。
            if not isinstance(loaded, Mapping):
                raise ValueError(
                    f'config file must contain a mapping, got '
                    f'{type(loaded).__name__}: {config_path}'
                )
            warn_duplicate_config_keys(
                getattr(loader, 'duplicate_keys', []),
                f'config file {config_path}',
                console=console,
                no_warnings=no_warnings,
            )
            warn_unknown_config_keys(
                loaded,
                KNOWN_CONFIG_KEYS,
                f'config file {config_path}',
                console=console,
                no_warnings=no_warnings,
            )
        else:
            raise ValueError(
                'Only YAML configuration files are supported.'
            )
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
        setup_process_config(config, loaded, console=console, no_warnings=no_warnings)
    return config

def warn_unknown_config_keys(
    mapping: Mapping,
    known_keys: list[str],
    where: str,
    console: Console | None = None,
    no_warnings: bool = False,
):
    '''
    Mapping に含まれる未知のキーを警告する。

    Args:
        mapping: 検査対象の Mapping (設定ファイルの読み込み結果)。
        known_keys: サポートされるキーの一覧。
        where: 警告メッセージに用いる対象の説明 (ファイルパスなど)。
        console: 警告の出力先。
        no_warnings: 警告を抑止するかどうか (--no-warnings 対応)。
    '''
    unknown = [str(key) for key in mapping.keys() if str(key) not in known_keys]
    if not unknown:
        return
    if console is None or no_warnings:
        return
    console.log(
        f'[yellow]warning: {where} contains unknown key(s) '
        f'{unknown}, which will be ignored. '
        f'Known keys: {known_keys}[/yellow]'
    )

def setup_process_config(
    config: Config,
    loaded: Mapping,
    console: Console | None = None,
    no_warnings: bool = False,
):
    dict_process = loaded.get('process')
    if isinstance(dict_process, Mapping):
        warn_unknown_config_keys(
            dict_process,
            KNOWN_PROCESS_KEYS,
            'process of the config file',
            console=console,
            no_warnings=no_warnings,
        )
        warn_wrong_typed_process_keys(
            dict_process,
            console=console,
            no_warnings=no_warnings,
        )
        # NOTE:
        #   以前は存在しない config.process に代入しており AttributeError になっていた。
        #   他の process 項目と同様、actions として登録する。
        dict_subprocess = dict_process.get('assign_length')
        if isinstance(dict_subprocess, Mapping):
            for key, value in flatten_row(dict_subprocess).items():
                config.actions.append(AssignLengthConfig(
                    target = key,
                    source = value,
                ))
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
    if should_be:
        raise ValueError(
            f'unsupported value type: {type(value)}, should be {should_be}, '
            f'value: {value}'
        )
    else:
        raise ValueError(
            f'unsupported value type: {type(value)}, value: {value}'
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
            array = value
            items = []
            for item in array:
                if isinstance(item, Mapping):
                    field = item.get('field')
                    optional = item.get('optional', False)
                    if field is None:
                        raise ValueError(
                            f'field is required for assign_array: {item}'
                        )
                    # NOTE:
                    #   YAML 上のキー名は 'field' だが、
                    #   dataclass 側の引数名は 'source' である。
                    items.append(AssignArrayElementConfig(
                        source = field,
                        optional = optional,
                    ))
                elif isinstance(item, str):
                    items.append(AssignArrayElementConfig(
                        source = item,
                        optional = True,
                    ))
                else:
                    raise_error_for_unsupported_type(item, 'dict or str')
            config.actions.append(AssignArrayConfig(
                target = key,
                items = items,
            ))
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
                raise ValueError(
                    f'field is required for filter: {item}'
                )
            operator = item.get('operator')
            if not operator:
                raise ValueError(
                    f'operator is required for filter: {item}'
                )
            # NOTE:
            #   真偽判定を使うと value: 0 や false が「必須欠落」と
            #   見なされてしまうため、キーの存在だけを確認する。
            if 'value' not in item:
                raise ValueError(
                    f'value is required for filter: {item}'
                )
            value = item['value']
            config.actions.append(FilterConfig(
                field = field,
                operator = operator,
                value = value,
            ))
        else:
            raise ValueError(
                f'unsupported filter item type: {type(item)}, item: {item}'
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
            config.actions.append(PushConfig(
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
                    raise ValueError(
                        f'field is required for split: {value}'
                    )
                delimiter = value.get('delimiter')
                if not delimiter:
                    raise ValueError(
                        f'delimiter is required for split: {value}'
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
                raise ValueError(
                    f'unsupported split value type: {type(value)}, value: {value}'
                )

def setup_pick_with_args(
    config: Config,
    list_fields: list[str],
    console: Console | None = None
):
    if console:
        console.log('list_fields:', list_fields)
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

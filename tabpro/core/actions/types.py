'''
This module contains the dataclasses that are used to define the configuration
for the table_converter package.
'''

from typing import (
    Any,
    Literal,
    Mapping,
    TypeAlias,
)

import dataclasses

from collections import (
    defaultdict,
)

@dataclasses.dataclass
class BaseActionConfig:
    pass

from .assign import AssignConfig

@dataclasses.dataclass
class AssignArrayElementConfig(BaseActionConfig):
    source: str
    optional: bool = True
@dataclasses.dataclass
class AssignArrayConfig(BaseActionConfig):
    target: str
    items: list[AssignArrayElementConfig]

@dataclasses.dataclass
class AssignConstantConfig(BaseActionConfig):
    target: str
    value: Any

@dataclasses.dataclass
class AssignFormatConfig(BaseActionConfig):
    target: str
    format: str

@dataclasses.dataclass
class AssignIdConfig(BaseActionConfig):
    target: str
    primary: list[str]
    context: list[str] | None = None
    reverse: bool = False

@dataclasses.dataclass
class AssignLengthConfig(BaseActionConfig):
    target: str
    source: str

@dataclasses.dataclass
class CastConfig(BaseActionConfig):
    target: str
    source: str
    as_type: Literal[
        'bool', 'int', 'float', 'str'
    ]
    required: bool = False
    assign_default: bool = False
    default_value: Any = None

@dataclasses.dataclass
class FilterConfig(BaseActionConfig):
    field: str
    operator: Literal[
        '==', '!=', '>', '>=', '<', '<=', '=~', 'not-in',
        'empty', 'not-empty',
    ]
    value: str | float | list[str]

@dataclasses.dataclass
class JoinConfig(BaseActionConfig):
    target: str
    source: str
    delimiter: str | None = None

@dataclasses.dataclass
class OmitConfig(BaseActionConfig):
    field: str
    purge: bool = False

@dataclasses.dataclass
class ParseConfig(BaseActionConfig):
    target: str
    source: str
    as_type: Literal[
        'bool', 'json', 'literal'
    ]
    required: bool = False
    assign_default: bool = False
    default_value: Any = None

@dataclasses.dataclass
class PickConfig(BaseActionConfig):
    target: str
    source: str

@dataclasses.dataclass
class PushConfig(BaseActionConfig):
    target: str
    source: str
    condition: str | None = None

from .replace_string import ReplaceConfig

@dataclasses.dataclass
class SplitConfig(BaseActionConfig):
    target: str
    source: str
    delimiter: str | None = None

# NOTE:
#   PEP 695 の `type` 文は Python 3.12 以降でしか使えないため、
#   3.10 以降をサポートするために TypeAlias による定義を用いる。
ContextColumnTuple: TypeAlias = tuple[str, ...]
ContextValueTuple: TypeAlias = tuple
PrimaryColumnTuple: TypeAlias = tuple[str, ...]
PrimaryValueTuple: TypeAlias = tuple

@dataclasses.dataclass
class IdMap:
    max_id: int = 0
    dict_value_to_id: Mapping[PrimaryValueTuple, int] = \
        dataclasses.field(default_factory=defaultdict)
    dict_id_to_value: Mapping[int, PrimaryValueTuple] = \
        dataclasses.field(default_factory=defaultdict)

IdContextMap: TypeAlias = Mapping[
    tuple[
        ContextColumnTuple,
        ContextValueTuple,
        PrimaryColumnTuple,
    ],
    IdMap
]

@dataclasses.dataclass
class GlobalStatus:
    id_context_map: IdContextMap = \
        dataclasses.field(default_factory=lambda: defaultdict(IdMap))

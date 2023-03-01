from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class MappingItem:
    source: str
    target: str


# @dataclass
# class ColumnsMapping:
#     _items: List[MappingItem]


@dataclass(frozen=True)
class TableInfo:
    schema: str
    table: str


@dataclass
class JobConfig:
    database: str
    schema: str
    table: str
    increment_column: str
    source: TableInfo
    target: TableInfo
    map: List[MappingItem]
    load_type: str = '_unknown_'
    pk: str = None

    def __post_init__(self):
        self.map = [MappingItem(**kv) for kv in self.map]
        self.target = TableInfo(**self.target)
        self.source = TableInfo(**self.source)
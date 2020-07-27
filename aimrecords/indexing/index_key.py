import re
from typing import List

from aimrecords.indexing.types import IndexArgType


class IndexKey(object):
    def __init__(self, index: IndexArgType):
        if not isinstance(index, tuple):
            index = (index, )
        self.raw_index = index
        self.validate(index)
        self.index = tuple(sorted(self.normalize(index)))

    def __hash__(self) -> int:
        return hash(self.index)

    def __eq__(self, other) -> bool:
        return hash(self.index) == hash(other)

    def __str__(self) -> str:
        return '-'.join(list(self.index))

    def get_keys(self) -> tuple:
        return self.raw_index

    @staticmethod
    def normalize(index: IndexArgType) -> List[str]:
        return list(map(lambda i: str(i), index))

    @staticmethod
    def validate(index: IndexArgType):
        if type(index) not in [str, int, tuple]:
            raise TypeError('index must be a type of str, int or tuple')

        for i in index:
            if type(i) not in [str, int]:
                TypeError('index field must be a type of str or int')

            if re.search('^[A-z0-9_:+]$', str(i)) is None:
                TypeError('index fields can contain only letters, numbers, ' +
                          ' underscore and `:`')

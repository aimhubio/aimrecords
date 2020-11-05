import re
from collections import OrderedDict
from typing import Optional
from base58 import b58encode, b58decode

from aimrecords.indexing.utils import normalize_type


class IndexKey(object):
    @staticmethod
    def validate(index: dict):
        if not isinstance(index, dict):
            raise TypeError('index must be a type of dict')

        for i in index.values():
            if type(i) not in [str, int]:
                TypeError('index field must be a type of str or int')

            if re.search('^[A-z0-9_:]+$', str(i)) is None:
                TypeError('index fields can contain only letters, numbers, ' +
                          ' underscore and `:`')

    @staticmethod
    def from_str(encoded_key) -> 'IndexKey':
        indices_items = b58decode(encoded_key).decode().split('-')
        indices = {}
        for item in indices_items:
            k, _, v = item.partition('=')
            indices.setdefault(k, normalize_type(v))
        return IndexKey(indices, encoded_key)

    def __init__(self, index: dict, str_repr: Optional[str] = None):
        self.validate(index)
        self.index = OrderedDict(sorted(index.items()))
        self.str_repr = str_repr

    def __hash__(self) -> int:
        return hash(tuple(self.index.items()))

    def __eq__(self, other) -> bool:
        return hash(self) == hash(other)

    def __str__(self) -> str:
        return self.to_str()

    def to_str(self) -> str:
        if self.str_repr is not None:
            return self.str_repr
        key = '-'.join(['{}={}'.format(i, j) for i, j in self.index.items()])
        return b58encode(key).decode()

    def get_fields(self):
        return tuple(self.index.items())

import re
from collections import OrderedDict
from base58 import b58encode


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

    def __init__(self, index: dict):
        self.validate(index)
        self.index = OrderedDict(sorted(index.items()))

    def __hash__(self) -> int:
        return hash(tuple(self.index.items()))

    def __eq__(self, other) -> bool:
        return hash(self) == hash(other)

    def __str__(self) -> str:
        key = '-'.join(['{}={}'.format(i, j) for i, j in self.index.items()])
        return b58encode(key).decode()

    def get_fields(self):
        return tuple(self.index.items())

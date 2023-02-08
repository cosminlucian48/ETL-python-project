from enum import Enum


class QueryType(Enum):
    HIGHEST = "highest"
    LOWEST = "lowest"

    @classmethod
    def is_valid(cls, query_type_string):
        return any(query_type_string == e.value for e in cls)

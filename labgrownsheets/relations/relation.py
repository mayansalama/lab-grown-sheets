from enum import Enum


class RelationType(Enum):
    ONE_TO_MANY = "one_to_many"
    MANY_TO_MANY = "many_to_many"


class Relation:
    def __init__(self, name, type=None, unique=False):
        self.name = name
        self.type = RelationType(type or "one_to_many")
        self.unique = bool(unique)

    @staticmethod
    def from_dict(d):
        return Relation(d['name'],
                        d.get('type'),
                        d.get("unique"))

    @property
    def id(self):
        return self.name + "_id"

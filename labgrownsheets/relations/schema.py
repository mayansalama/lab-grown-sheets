from typing import List


class SchemaField:
    def __init__(self, name, type=None, primary_key=False, parent_entity=None, mutating=False):
        self.name = name
        if not type:
            self.type = lambda x: x
        else:
            self.type = type
        self.primary_key = primary_key
        self.parent_entity = parent_entity
        self.mutating = mutating

    def __str__(self):
        return self.name

    @staticmethod
    def from_dict(d):
        return SchemaField(d['name'],
                           d.get('type'),
                           d.get('primary_key'),
                           d.get('parent_entity'),
                           d.get('mutating'))


class Schema:
    def __init__(self, schema_fields):
        self.fields: List[SchemaField] = schema_fields

    def __bool__(self):
        return len(self.fields) != 0

    @staticmethod
    def from_list(l):
        return Schema([SchemaField.from_dict(field) for field in l])

    @property
    def primary_keys(self):
        return [f for f in self.fields if f.primary_key]

    def get_primary_key(self):
        keys = self.primary_keys
        if len(keys) > 1:
            # FIXME(): Should we allow more than one primary key?
            raise ValueError("More than one primary key")
        elif len(keys) == 1:
            return str(keys[0])
        else:
            return None

    def get_fields_for_parent(self, parent):
        return [f for f in self.fields if f.parent_entity == parent]

    @property
    def mutating_cols(self):
        return [f for f in self.fields if f.mutating]

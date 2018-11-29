import inspect
from abc import ABC, abstractmethod

from labgrownsheets.relations.relation import Relation, RelationType
from labgrownsheets.relations.schema import Schema


class BaseProfiler(ABC):

    def __init__(self, name, num_iterations, num_entities_per_iteration=None, relations=None, schema=None,
                 kwds=None):
        self.name = name
        self.num_iterations = num_iterations
        if not num_entities_per_iteration:
            num_entities_per_iteration = 1
        self.num_entities_per_iteration = num_entities_per_iteration
        self.relations = relations
        self.schema = schema or []
        self.kwds = kwds

    def base_arg_list(self):
        return {'name': self.name,
                'num_iterations': self.num_iterations,
                'num_entities_per_iteration': self._num_facts_per_iter,
                'relations': self.relations,
                'schema': self.schema,
                'kwds': self.kwds}

    @classmethod
    def init_handler(cls, init_vals):
        if isinstance(init_vals, dict):
            return cls.from_dict(init_vals)
        elif isinstance(init_vals, list):
            return cls.from_list(init_vals)
        else:  # Duck typing
            return init_vals

    ############################################################################
    # Schema handling
    ############################################################################

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, schema):
        if isinstance(schema, list):
            self._schema = Schema.from_list(schema)
        else:
            self._schema = schema  # duck typing

    @property
    def id(self):
        key = self.schema.get_primary_key()
        if not key:
            return self.name + "_id"
        return key

    ############################################################################
    # Relation and scd handling
    ############################################################################

    @property
    def preserve_id_across_its(self):
        return False

    def reset(self):
        pass

    @property
    def relations(self):
        return self._relations

    @relations.setter
    def relations(self, relations):
        if relations and isinstance(relations, list):
            self._relations = [Relation.from_dict(rel) if isinstance(rel, dict) else rel for rel in relations]
        else:
            self._relations = []

    @property
    def one_to_many_relations(self):
        return [rel for rel in self.relations if rel.type == RelationType("one_to_many")]

    @property
    def many_to_many_relations(self):
        return [rel for rel in self.relations if rel.type == RelationType("many_to_many")]

    ############################################################################
    # Utils to handle generator functions
    ############################################################################

    @staticmethod
    def _check_if_gen(val):
        test = val()
        if inspect.isgenerator(test):
            return test
        else:
            return val

    @staticmethod
    def _return_executable(val):
        if inspect.isgenerator(val):
            return lambda: next(val)
        return val

    @property
    def num_entities_per_iteration(self):
        return self._return_executable(self._num_facts_per_iter)()

    @num_entities_per_iteration.setter
    def num_entities_per_iteration(self, val):
        if not callable(val):
            if str(val).isnumeric():
                val = int(str(val))
                func = lambda: val
            else:
                raise ValueError("Num entities per iteration must be either numeric or a function")
        else:
            func = self._check_if_gen(val)

        # FIXME(): Check that generator value returns int then preserve result for next call
        if not inspect.isgenerator(func) and not isinstance(func(), int):  # Not checking gens
            raise ValueError("Num facts per iteration must return an integer")
        self._num_facts_per_iter = func

    ############################################################################
    # Abstract methods
    ############################################################################

    @abstractmethod
    def generate_entity(self, *args, **kwargs):
        pass

    @classmethod
    def process_base_dict_args(cls, d):
        name = d['name']
        num_iterations = d['num_iterations']
        relations = d.get('relations', [])
        num_entities_per_iteration = d.get('num_entities_per_iteration')
        schema = d.get('schema')

        return {'name': name,
                'num_iterations': num_iterations,
                'num_entities_per_iteration': num_entities_per_iteration,
                'relations': relations,
                'schema': schema,
                'kwds': d}

    @classmethod
    def from_dict(cls, d):  # Optional to implement
        raise NotImplementedError("Profiler {} is not configured to initiate from a dictionary".format(
            cls.__name__))

    @classmethod
    def from_list(cls, l):  # Optional to implement
        raise NotImplementedError("Profiler {} is not configured to initiate from a list".format(
            cls.__name__))

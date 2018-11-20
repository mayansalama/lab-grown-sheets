from unittest import TestCase

from labgrownsheets.profilers import *
from labgrownsheets.relations.schema import Schema
from labgrownsheets.relations.relation import Relation, RelationType

base_dict = lambda: {
    'name': 'test',
    'entity_generator': lambda: {'col1': 'always1'},
    'num_iterations': 1,
    'relations': [{'name': 'test2'},
                  {'name': 'test3', 'type': 'many_to_many', 'unique': True},
                  Relation("test4")]
}


class TestBaseProfiler(TestCase):

    def test_init__handler(self):
        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(base_dict())

        assert from_dict.name == 'test'
        assert from_dict.generate_entity() == {'col1': 'always1'}
        assert from_dict.num_iterations == 1
        assert from_dict.num_entities_per_iteration == 1

        # Relations
        assert issubclass(from_dict.relations[0].__class__, Relation)
        assert from_dict.relations[0].name == 'test2'
        assert from_dict.relations[0].type == RelationType('one_to_many')
        assert not from_dict.relations[0].unique
        assert from_dict.relations[1].name == 'test3'
        assert from_dict.relations[1].type == RelationType('many_to_many')
        assert from_dict.relations[1].unique
        assert from_dict.relations[2].name == 'test4'

        assert from_dict.many_to_many_relations
        assert from_dict.one_to_many_relations

        # Schema
        assert issubclass(from_dict.schema.__class__, Schema)

        # TODO: Test from list when there's a usecase for it

    def test_init__num_ents_per_it_static(self):
        num_ents = base_dict()

        num_ents['num_entities_per_iteration'] = '1'
        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(num_ents)
        assert from_dict.num_iterations == 1

        num_ents['num_entities_per_iteration'] = 1
        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(num_ents)
        assert from_dict.num_iterations == 1

        with self.assertRaises(ValueError):
            num_ents['num_entities_per_iteration'] = 1.0
            from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(num_ents)

        with self.assertRaises(ValueError):
            num_ents['num_entities_per_iteration'] = 1.5
            from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(num_ents)

    def test_init__num_ents_per_it_dynamic(self):
        num_ents = base_dict()

        num_ents['num_entities_per_iteration'] = lambda: 1
        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(num_ents)
        assert from_dict.num_iterations == 1

        def gen(): yield 1

        num_ents['num_entities_per_iteration'] = gen
        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(num_ents)
        assert from_dict.num_iterations == 1


class TestNaiveProfiler(TestCase):

    def test_func_entity_generator(self):
        func_ent: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(base_dict())
        assert func_ent.generate_entity() == {'col1': 'always1'}

    def test_generator_entity_generator(self):
        def gen():
            for i in range(10):
                yield (i)

        d = base_dict()
        d['entity_generator'] = gen
        gen_ent: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(d)
        assert gen_ent.generate_entity() == 0
        assert gen_ent.generate_entity() == 1

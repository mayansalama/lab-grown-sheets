import random
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

    def test_entity_generator__function(self):
        func_ent: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(base_dict())
        assert func_ent.generate_entity() == {'col1': 'always1'}

    def test_entity_generator__generator(self):
        def gen():
            for i in range(10):
                yield (i)

        d = base_dict()
        d['entity_generator'] = gen
        gen_ent: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(d)
        assert gen_ent.generate_entity() == 0
        assert gen_ent.generate_entity() == 1


scd_num_ents = 10000
scd_mutate_rate = 0.1


class TestSCDType2Profiler(TestCase):

    def test_generation(self):
        d = base_dict()
        d['mutation_rate'] = scd_mutate_rate
        d['num_iterations'] = scd_num_ents

        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(d)

        assert not hasattr(from_dict, '_reset')
        assert not from_dict.preserve_id_across_its

        scd_type2 = BaseScdProfiler(from_dict)
        assert hasattr(scd_type2, '_reset')
        assert scd_type2.preserve_id_across_its

        # Assert that we are generating approx correct number of mutations
        num_ents_per_it = [scd_type2.num_entities_per_iteration() for i in range(scd_num_ents)]
        mean = 1 / scd_mutate_rate
        actual_mean = sum(num_ents_per_it) / len(num_ents_per_it)

        assert abs((mean - actual_mean) / mean) < 0.05  # 5% tol

    def test_mutating_cols_behaviour(self):
        d = base_dict()
        d['mutation_rate'] = scd_mutate_rate
        d['num_iterations'] = scd_num_ents
        d['entity_generator'] = lambda: {'col1': random.random(), 'col2': random.random(), 'col3': random.random()}

        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(d)
        scd_type2 = BaseScdProfiler(from_dict)

        # Test default "all" behaviour mutates all columns
        res1 = scd_type2.generate_entity()
        res2 = scd_type2.generate_entity()
        res3 = scd_type2.generate_entity()

        for k in res1:
            assert res1[k] != res2[k]
            assert res1[k] != res3[k]

        # We can also specify in schema or in the input dictionary
        d['mutating_cols'] = ['col2']
        d['schema'] = [{'name': 'col3', 'mutating': True}]

        from_dict: NaiveProfiler = str_to_class("NaiveProfiler").init_handler(d)
        scd_type2 = BaseScdProfiler(from_dict)

        res1 = scd_type2.generate_entity()
        res2 = scd_type2.generate_entity()
        res3 = scd_type2.generate_entity()

        assert res1['col1'] == res2['col1'] and res1['col1'] == res3['col1']
        assert res1['col2'] != res2['col2'] and res1['col2'] != res3['col2'] and res2['col2'] != res3['col2']
        assert res1['col3'] != res2['col3'] and res1['col3'] != res3['col3'] and res2['col3'] != res3['col3']

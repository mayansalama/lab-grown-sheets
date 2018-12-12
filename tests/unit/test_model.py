import yaml
import os
from copy import deepcopy
from unittest import TestCase

from labgrownsheets.model import *

TEST_SIZE = 1000


def customer_gen():
    name = 0
    while True:
        yield {'name': name}
        name += 1


def order_gen():
    for i in range(TEST_SIZE):
        yield {'order_amount': i}


def order_item_gen():
    while True:
        for i in range(10):
            yield {'product_val': i}


basic_model = [
    ('naive', {'name': 'customer',
               'num_iterations': TEST_SIZE,
               'entity_generator': customer_gen}),
    ('naive', {'name': 'order',
               'num_iterations': TEST_SIZE,
               'entity_generator': order_gen,
               'relations': [{'name': 'customer', 'type': 'one_to_many', 'unique': False}]}),
    ('naive', {'name': 'order_item',
               'num_iterations': 1,
               'entity_generator': order_item_gen,
               'num_entities_per_iteration': 10,
               'relations': [{'name': 'order', 'type': 'one_to_many', 'unique': False}]})
]


class TestModel(TestCase):

    def test_dag__loop(self):
        broken_model = deepcopy(basic_model)
        broken_model.append(
            ('naive', {
                'name': 'contrived',
                'num_iterations': 1,
                'entity_generator': lambda: 1,
                'relations': [{'name': 'order'}]
            })
        )
        broken_model[0][1]['relations'] = [{'name': 'customer'}]

        model = StarSchemaModel.from_list(broken_model)

        with self.assertRaises(ValueError):
            model.generate_dag()

    def test_basic_model_generation__one_to_many_unique__multi_its(self):
        model = StarSchemaModel.from_list(basic_model)
        model.generate_all_datasets()
        datasets = model.datasets

        custs = datasets['customer']
        assert len(custs) == TEST_SIZE
        assert {n['name'] for v in custs.values() for n in v} == {i for i in range(TEST_SIZE)}

        orders = datasets['order']
        for order_vals in orders.values():
            for val in order_vals:
                assert val['customer_id'] in custs

        assert {n['order_amount'] for v in orders.values() for n in v} == {i for i in range(TEST_SIZE)}

        order_items = datasets['order_item']
        assert len(order_items) == 10
        y = {n['product_val'] for v in order_items.values() for n in v}
        assert {n['product_val'] for k in order_items.values() for n in k} == {i for i in range(10)}
        order_ids = [n['order_id'] for v in order_items.values() for n in v]
        assert len(order_ids) == 10
        assert len(set(order_ids)) == 1

    def test_model_generation_unique_one_to_many(self):
        one_to_many_unique = deepcopy(basic_model)
        one_to_many_unique[1][1]['relations'] = [{'name': 'customer', 'type': 'one_to_many', 'unique': True}]
        model = StarSchemaModel.from_list(one_to_many_unique)
        model.generate_all_datasets()
        datasets = model.datasets

        id_ct = {}
        for cust_id in datasets['customer']:
            for orders in datasets['order'].values():
                for order_val in orders:
                    if cust_id in order_val['customer_id']:
                        if not id_ct.get(cust_id):
                            id_ct[cust_id] = 0
                        id_ct[cust_id] += 1

        for ct in id_ct.values():
            assert ct == 1

    def test_model_generation_unique_many_to_many(self):
        many_to_many_unique = deepcopy(basic_model)
        many_to_many_unique[2][1]['relations'] = [{'name': 'order', 'type': 'many_to_many', 'unique': True}]

        model = StarSchemaModel.from_list(many_to_many_unique)
        model.generate_all_datasets()
        datasets = model.datasets

        order_items = datasets['order_item']
        order_ids = [order_item_vals['order_id'] for v in order_items.values() for order_item_vals in v]
        assert len(order_ids) == 10
        assert len(set(order_ids)) == 10

    def test_model_generation_with_schema(self):
        # basic_model_has_int
        model = StarSchemaModel.from_list(basic_model)
        model.generate_all_datasets()
        datasets = model.datasets

        orders = datasets['order']
        for orders in orders.values():
            for order in orders:
                assert isinstance(order['order_amount'], int)

        type_change = deepcopy(basic_model)
        type_change[1][1]['schema'] = [{'name': 'order_amount', 'type': float}]
        model = StarSchemaModel.from_list(type_change)
        model.generate_all_datasets()
        datasets = model.datasets

        orders = datasets['order']
        for orders in orders.values():
            for order in orders:
                assert isinstance(order['order_amount'], float)

    def test_model_with_different_primary_key(self):
        pk = deepcopy(basic_model)
        pk[1][1]['schema'] = [{'name': 'orders_key', 'primary_key': True}]
        model = StarSchemaModel.from_list(pk)
        model.generate_all_datasets()
        datasets = model.datasets

        orders = datasets['order']
        for orders in orders.values():
            for order in orders:
                assert order["orders_key"] is not None
                assert not order.get('order_id')

    def test_model_with_scd_type2_parent_has_links(self):
        scd = deepcopy(basic_model)
        scd[0] = ('naive_type2_scd', {'name': 'customer',
                                      'num_iterations': TEST_SIZE,
                                      'entity_generator': customer_gen,
                                      'mutation_rate': 0.9})
        model = StarSchemaModel.from_list(scd)
        model.generate_all_datasets()
        datasets = model.datasets

        for orders in datasets['order'].values():
            for order_val in orders:
                assert order_val['customer_id'] in datasets['customer']


class TestAdapters(TestCase):

    def test_postgres_adapter(self):
        model = StarSchemaModel.from_list(basic_model)
        model.generate_all_datasets()

        psa = PostgresSchemaAdapter(model)
        psa.to_dbt_schema()

        with open(psa.name + ".yml", 'r+') as f:
            contents = yaml.load(f)

        assert type(contents) == dict

        assert contents == {
            'customer': {'column_types': {'customer_id': 'text', 'name': 'int'}},
            'order': {'column_types': {'customer_id': 'text', 'order_amount': 'int', 'order_id': 'text'}},
            'order_item': {'column_types': {'order_id': 'text', 'order_item_id': 'text', 'product_val': 'int'}}
        }

        os.remove(psa.name + ".yml")

    def test_big_query_adapter(self):
        model = StarSchemaModel.from_list(basic_model)
        model.generate_all_datasets()

        bqa = BigquerySchemaAdapter(model)
        bqa.to_dbt_schema()

        with open(bqa.name + ".yml", 'r+') as f:
            contents = yaml.load(f)

        assert type(contents) == dict

        assert contents == {
            'customer': {'column_types': {'customer_id': 'string', 'name': 'integer'}},
            'order': {'column_types': {'customer_id': 'string', 'order_amount': 'integer', 'order_id': 'string'}},
            'order_item': {'column_types': {'order_id': 'string', 'order_item_id': 'string', 'product_val': 'integer'}}
        }

        os.remove(bqa.name + ".yml")

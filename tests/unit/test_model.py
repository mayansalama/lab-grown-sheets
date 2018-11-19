from copy import deepcopy
from unittest import TestCase

from labgrownsheets.model import StarSchemaModel

TEST_SIZE = 1000


def customer_gen():
    for name in range(TEST_SIZE):
        yield {'name': name}


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
        assert {n['name'] for n in custs.values()} == {i for i in range(TEST_SIZE)}

        orders = datasets['order']
        for order_id, order_vals in orders.items():
            assert order_vals['customer_id'] in custs

        assert {n['order_amount'] for n in orders.values()} == {i for i in range(TEST_SIZE)}

        order_items = datasets['order_item']
        assert len(order_items) == 10
        y = {n['product_val'] for n in order_items.values()}
        assert {n['product_val'] for n in order_items.values()} == {i for i in range(10)}
        order_ids = [order_item_vals['order_id'] for order_item_vals in order_items.values()]
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
            for order_id, order_val in datasets['order'].items():
                if cust_id in order_val['customer_id']:
                    if not id_ct.get(cust_id):
                        id_ct[cust_id] = 0
                    id_ct[cust_id] += 1

        for ct in id_ct.values():
            assert ct == 1

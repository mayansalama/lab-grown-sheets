import random
import os
from contextlib import contextmanager
from unittest import TestCase

from labgrownsheets.model import StarSchemaModel
from labgrownsheets.profilers import NaiveProfiler, SamplingProfiler


@contextmanager
def sample_file_with_cols(cols, num_its=10, val=None, name='test'):
    def gen_func():
        return {col: random.random() if not val else val for col in cols}

    np = NaiveProfiler(name=name, num_iterations=num_its, generator_funtion=gen_func)
    gen = StarSchemaModel([np])
    gen.generate_all_datasets()
    gen.to_csv()
    yield name + '.csv'
    os.remove(name + '.csv')


class TestSamplingProfiler(TestCase):

    def test_sample_csv_genner(self):
        with sample_file_with_cols(['col1', 'col2']) as x:
            assert os.path.exists(x)
        assert not os.path.exists(x)

    def test_from_dict_and_all_cols(self):
        with sample_file_with_cols(['col1', 'col2'], val='testing') as x:
            sp = SamplingProfiler.from_dict({'name': 'sampler_test', 'num_iterations': 1000, 'file_path': x})
            gen = StarSchemaModel([sp])
            gen.generate_all_datasets()
            dataset = gen.datasets['sampler_test']

            assert len(dataset) == 1000
            for list_vals in dataset.values():
                for vals in list_vals:
                    assert vals['col1'] == 'testing'
                    assert vals['col2'] == 'testing'

    def test_partial_cols(self):
        with sample_file_with_cols(['col1', 'col2']) as x:
            sp = SamplingProfiler(name='sampler_test', num_iterations=1000, file_path=x, sample_cols=['col1'])
            gen = StarSchemaModel([sp])
            gen.generate_all_datasets()
            dataset = gen.datasets['sampler_test']

            assert len(dataset) == 1000
            for list_vals in dataset.values():
                for vals in list_vals:
                    assert vals['col1']
                    assert 'col2' not in vals

    def test_invalid_cols(self):
        with sample_file_with_cols(['col1', 'col2'], val='testing') as x:
            with self.assertRaises(ValueError):
                SamplingProfiler(name='sampler_test', num_iterations=1000, file_path=x, sample_cols=['col3'])

            with self.assertRaises(ValueError):
                SamplingProfiler(name='sampler_test', num_iterations=1000, file_path=x, sample_cols=['col1', 'col3'])

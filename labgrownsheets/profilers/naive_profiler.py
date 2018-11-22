from labgrownsheets.profilers.base_profiler import BaseProfiler


class NaiveProfiler(BaseProfiler):

    def __init__(self, generator_funtion, *args, **kwargs):
        self.gen = generator_funtion
        super().__init__(*args, **kwargs)

    @classmethod
    def from_dict(cls, d):
        gen = d['entity_generator']
        return NaiveProfiler(gen, **cls.process_base_dict_args(d))

    @property
    def gen(self):
        return self._return_executable(self._gen)

    @gen.setter
    def gen(self, val):
        self._gen = self._check_if_gen(val)

    def generate_entity(self, *args, **kwargs):
        return self.gen()

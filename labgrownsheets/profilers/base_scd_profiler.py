import datetime
from copy import deepcopy

from numpy.random import geometric, uniform

from labgrownsheets.profilers.base_profiler import BaseProfiler

DEFAULT_HIGH_DATE = datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)
DEFAULT_MIN_VALID_FROM = datetime.datetime.now() - datetime.timedelta(days=365)
DEFAULT_MAX_VALID_FROM = datetime.datetime.now()


class ScdProfiler(BaseProfiler):

    #############################################
    # Base Profiler methods
    #############################################

    def __init__(self, profiler):
        self.profiler = profiler
        self._num_ents = 1

        base_arg_list = profiler.base_arg_list()
        base_arg_list['num_entities_per_iteration'] = self.yield_num_ents
        super().__init__(**base_arg_list)

        self.mutation_rate = float(self.kwds['mutation_rate'])
        self.min_valid_from = self.kwds.get('min_valid_from', DEFAULT_MIN_VALID_FROM)
        self.max_valid_from = self.kwds.get('max_valid_from', DEFAULT_MAX_VALID_FROM)
        self.high_date = self.kwds.get('high_date', DEFAULT_HIGH_DATE)
        self.mutating_cols = self.get_mutating_cols()

        self.next_valid_from_to = self.yield_valid_froms()
        self.next_is_new_ent = self.yield_is_new_entity()

    @property
    def preserve_id_across_its(self):
        return True

    def get_mutating_cols(self):
        mutating_cols = self.kwds.get('mutating_cols', [])
        mutating_cols = set(mutating_cols) | {f.name for f in self.schema.mutating_cols}
        return mutating_cols or "all"

    #############################################
    # CDC Handling
    #############################################

    def yield_num_ents(self):
        for i in geometric(1 - self.mutation_rate, self.num_iterations):
            self._num_ents = int(i)
            yield (int(i))

    def yield_valid_froms(self):
        while True:
            rng = sorted(uniform(self.min_valid_from.timestamp(), self.max_valid_from.timestamp(), self._num_ents))
            for x in range(len(rng) - 1):
                yield datetime.datetime.fromtimestamp(rng[x]), datetime.datetime.fromtimestamp(rng[x + 1])
            yield datetime.datetime.fromtimestamp(rng[-1]), self.high_date

    def yield_is_new_entity(self):
        while True:
            for i in range(self._num_ents):
                yield i == 0

    def is_new_entity(self):
        return next(self.next_is_new_ent)

    def generate_entity(self, *args, **kwargs):
        if self.is_new_entity():
            res = self.profiler.generate_entity(*args, **kwargs)
            self._last_res = deepcopy(res)
        else:
            new_res = self.profiler.generate_entity(*args, **kwargs)
            res = {k: new_res[k] if k in self.mutating_cols or self.mutating_cols == "all" else v
                   for k, v in self._last_res.items()}

        vf, vt = next(self.next_valid_from_to)
        res['valid_from_timestamp'] = vf
        res['valid_to_timestamp'] = vt
        return res

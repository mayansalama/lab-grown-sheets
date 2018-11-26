from numpy.random import geometric

from labgrownsheets.profilers import BaseProfiler


class BaseScdProfiler:

    #############################################
    # Base Profiler methods
    # TODO() Put in valid from/to handlers via Relation "mutation object"
    #############################################

    def __init__(self, profiler):
        self.profiler: BaseProfiler = profiler
        self.yield_num_ents = profiler.num_iterations
        self._reset = True

    @property
    def preserve_id_across_its(self):
        return True

    def reset(self):
        self._reset = True

    @property
    def yield_num_ents(self):
        return self._gen_ents

    @yield_num_ents.setter
    def yield_num_ents(self, num_its):
        self._gen_ents = iter(geometric(self.mutation_rate, num_its))

    def num_entities_per_iteration(self):
        return next(self.yield_num_ents)

    #############################################
    # CDC Handling
    #############################################

    @property
    def mutation_rate(self):
        rate = self.profiler.kwds['mutation_rate']
        return float(rate)

    @property
    def mutating_cols(self):
        mutating_cols = self.profiler.kwds.get('mutating_cols', [])
        mutating_cols = set(*mutating_cols, *self.profiler.schema.mutating_cols)
        return mutating_cols or "all"

    def generate_entity(self, *args, **kwargs):
        if self.reset():
            self._reset = False
            self._last_res = self.profiler.generate_entity(*args, **kwargs)
            return self._last_res
        else:
            new_res = self.profiler.generate_entity(*args, **kwargs)
            mutating_cols = self.mutating_cols
            return {k: new_res[k] if v in mutating_cols or mutating_cols == "all" else v
                    for k, v in self._last_res.items()}

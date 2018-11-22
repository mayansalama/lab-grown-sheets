from labgrownsheets.profilers.naive_profiler import NaiveProfiler
from labgrownsheets.profilers.base_profiler import BaseProfiler
from labgrownsheets.profilers.sampling_profiler import SamplingProfiler


def str_to_class(class_name):
    for cls, names in _class_to_str.items():
        if class_name.lower() in names:
            return cls


def rootword_plus_endings(word):
    endings = ['', '_profiler', 'profiler']
    return [word + e for e in endings]


_class_to_str = {
    NaiveProfiler: rootword_plus_endings("naive"),
    SamplingProfiler: rootword_plus_endings("sampling") + rootword_plus_endings("sample")
}

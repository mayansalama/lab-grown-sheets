from labgrownsheets.profilers.naive_profiler import NaiveProfiler
from labgrownsheets.profilers.base_profiler import BaseProfiler


def str_to_class(class_name):
    for cls, names in _class_to_str.items():
        if class_name.lower() in names:
            return cls


_class_to_str = {
    NaiveProfiler: ["naive", "naive_profiler", "naiveprofiler"],
    BaseProfiler: ["base", "base_profiler", "baseprofiler"]
}

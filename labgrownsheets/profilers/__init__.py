__all__ = ['resolve_profiler', 'NaiveProfiler', 'SamplingProfiler', 'ScdProfiler']

from labgrownsheets.profilers.naive_profiler import NaiveProfiler
from labgrownsheets.profilers.sampling_profiler import SamplingProfiler
from labgrownsheets.profilers.base_scd_profiler import ScdProfiler


def str_to_class(class_name):
    for cls, names in class_to_str.items():
        if class_name.lower() in names:
            return cls


def rootword_plus_endings(word):
    endings = ['', '_profiler', 'profiler']
    return [word + e for e in endings]


class_to_str = {
    NaiveProfiler: rootword_plus_endings("naive"),
    SamplingProfiler: rootword_plus_endings("sampling") + rootword_plus_endings("sample")
}


def fix_name(name):
    name = name.strip()
    for fix in type2_fixes:
        if name.startswith(fix + "_"):
            return name.replace(fix + "_", "")
        elif name.endswith("_" + fix):
            return name.replace("_" + fix, "")
        else:
            return name


def is_scd_type_2(name):
    for fix in type2_fixes:
        if name.startswith(fix + "_") or name.endswith("_" + fix):
            return True
        return False


type2_fixes = ['type2_scd', 'scd_type2']


def resolve_profiler(profiler_name, profiler_args):
    cleaned_name = fix_name(profiler_name)
    cls = str_to_class(cleaned_name)
    if not cls:
        raise ValueError("Unable to identify class " + profiler_name)
    inst = cls.init_handler(profiler_args)

    if is_scd_type_2(profiler_name):
        inst = ScdProfiler(inst)
    return inst

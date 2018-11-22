import csv
import random
from enum import Enum
from typing import Dict, List

from labgrownsheets.profilers import base_profiler


class ReadableType(Enum):
    CSV = 'CSV'


class CSVReader:
    # FIXME(): Replace Sniffer with optional extract from Profiler init kwargs - maybe use mixins instead? Kinda ugly
    @staticmethod
    def csv_to_data(file_path):
        data = []
        with open(file_path, 'r') as csvfile:
            sample = csvfile.read(1024)
            has_header = csv.Sniffer().has_header(sample)
            dialect = csv.Sniffer().sniff(sample)
            csvfile.seek(0)
            reader = csv.reader(csvfile, dialect)
            for num, row in enumerate(reader):
                if not num:
                    if has_header:
                        headers = row
                    else:
                        headers = ['col' + str(i) for i in range(len(row))]
                if num or (not num and not has_header):
                    data.append(dict(zip(headers, row)))

        return data


filetype_to_data = {
    ReadableType.CSV: CSVReader.csv_to_data
}


class SamplingProfiler(base_profiler.BaseProfiler):

    def __init__(self, file_path, file_type=None, sample_cols=None, *args, **kwargs):
        self.file_path = file_path
        file_type = file_type or 'CSV'
        self.file_type = ReadableType(file_type.upper())
        super().__init__(*args, **kwargs)

        self.data: List[Dict] = self.read_file()
        self.cols = sample_cols or 'all'

    @classmethod
    def from_dict(cls, d):
        file_path = d['file_path']
        file_type = d.get('file_type')
        sample_cols = d.get('sample_cols')
        return SamplingProfiler(file_path, file_type, sample_cols, **cls.process_base_dict_args(d))

    @property
    def cols(self):
        return self._cols

    @cols.setter
    def cols(self, val):
        all_cols = self.data[0].keys()
        if hasattr(val, 'lower') and val.lower() == 'all':
            self._cols = all_cols
        elif set(val).issubset(all_cols):
            self._cols = val
        else:
            raise ValueError("Column set {} is not a subset of column set {}".format(val, all_cols))

    def read_file(self):
        # Map between file path and source data type - should return list of dict, each dict being a row
        return filetype_to_data[self.file_type](self.file_path)

    def generate_entity(self, *args, **kwargs):
        data = random.sample(self.data, 1)[0]
        return {col: val for col, val in data.items() if col in self.cols}

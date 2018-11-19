import os
import csv
import json
import pickle
from datetime import datetime, date

NUM_DOTS = 20


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class Savers:

    ##################################################################
    # Save File
    ##################################################################
    @staticmethod
    def create_path(path):
        if path and not os.path.exists(path):
            os.makedirs(path)

    def to_csv(self, path):
        self.create_path(path)
        for name, uids in self.datasets.items():
            with open(os.path.join(path, name + ".csv"), "w+") as f:
                wr = csv.writer(f)
                headers = True
                for row in uids.values():
                    if headers:
                        headers = False
                        wr.writerow(list(row.keys()))

                    wr.writerow(list(row.values()))

    def to_json(self, path):
        self.create_path(path)
        for name, uids in self.datasets.items():
            with open(os.path.join(path, name + ".json"), "w+") as f:
                json.dump(list(uids.values()), f, default=json_serial)

    def to_schemas(self, path):
        self.create_path(path)
        for name, uids in self.datasets.items():
            with open(os.path.join(path, name + ".schema"), "wb") as f:
                first_row = next(iter(uids.values()))
                schema = {}
                for name, val in first_row.items():
                    schema[name] = type(val)
                pickle.dump(schema, f)

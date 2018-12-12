import os
import yaml
import datetime

from labgrownsheets.model import StarSchemaModel

NUM_DOTS = 20


class BaseSchemaAdapter:
    name = "BaseSchema"
    overwritten_conversions = ((None, None),)
    default_conversions = (
        (str, "text"),
        (float, "real"),
        (int, "int"),
        (datetime.datetime, "timestamp")
    )

    def __init__(self, model):
        self.model = model

    def to_dbt_schema(self, path='', name=''):
        """ Returns a dbt schema dict, which can be used by seed schemas

        :param path: directory to create folder in
        :param name: defaults to name of adapter
        """
        StarSchemaModel.create_path(path)

        if not name:
            name = self.name
        schemas = {}
        for model_name, uids in self.model.datasets.items():
            first_row = next(iter(uids.values()))[0]
            schema = {}
            for col_name, val in first_row.items():
                schema[col_name] = self.convert_pytype(type(val))
            schemas[model_name] = {'column_types': schema}

        with open(os.path.join(path, name + ".yml"), "w+") as f:
            yaml.dump(schemas, f, default_flow_style=False)

    def convert_pytype(self, dtype):
        overwritten = [t[1] for t in self.overwritten_conversions if dtype == t[0]]
        if overwritten:
            return overwritten[0]
        else:
            return [t[1] for t in self.default_conversions if dtype == t[0]][0]


class PostgresSchemaAdapter(BaseSchemaAdapter):
    name = "PostgresSchema"


class BigquerySchemaAdapter(BaseSchemaAdapter):
    name = "BigquerySchema"
    overwritten_conversions = (
        (str, "string"),
        (float, "float"),
        (int, "integer")
    )

import os
import csv
import json
import pickle
import random
import uuid
from typing import Dict
from datetime import datetime, date

import networkx

from labgrownsheets.profilers import resolve_profiler
from labgrownsheets.profilers.base_profiler import BaseProfiler

NUM_DOTS = 20


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class StarSchemaModel:

    ##################################################################
    # Init and props
    ##################################################################

    def __init__(self, entity_list):
        self.entity_dict: Dict[BaseProfiler] = {
            entity.name: entity for entity in entity_list
        }
        self.dag = None
        self.datasets = None

    def add_entity(self, entity):
        # FIXME(): Add in smarts to only regenerate related entities
        self.entity_dict[entity.name] = entity
        self.dag = None

    @classmethod
    def from_list(cls, l):
        # This is a list of tuples = (profiler type, values)
        return StarSchemaModel([resolve_profiler(val[0], val[1]) for val in l])

    ##################################################################
    # DAG Handling
    ##################################################################

    def generate_dag(self):
        dag = networkx.DiGraph()

        # Add our nodes
        for entity in self.entity_dict.values():
            for relation in entity.relations:
                # entity is child of relation
                try:
                    dag.add_edge(self.entity_dict[relation.name], entity)
                except KeyError as e:
                    raise KeyError("Unable to find relation: '{}'".format(str(e)))
            dag.add_node(entity)  # Just in case there's a standalone

        if not networkx.is_directed_acyclic_graph(dag):
            raise ValueError("Circular dependencies in relations")

        return dag

    def generate_all_datasets(self, print_progress=False):
        if not self.dag:
            self.dag = self.generate_dag()

        datasets = {}
        max_name_length = len(max(self.entity_dict.keys(), key=len))

        for entity in networkx.topological_sort(self.dag):
            if print_progress:
                print("Generating entity {}{}  ".format(entity.name, ' ' * (max_name_length - len(entity.name))),
                      end="", flush=True)
            datasets[entity.name] = self.generate_entity_data(entity, datasets, entity.num_iterations, print_progress)

        self.datasets = datasets

    def yield_entities(self, print_progress=False, **kwargs):
        if not self.dag:
            self.generate_dag()

        new_entities = {}
        for entity_name, number_iterations in kwargs.items():
            new_entities[entity_name] = self.generate_entity_data(self.entity_dict[entity_name], self.datasets,
                                                                  number_iterations, print_progress)

    ##################################################################
    # Create Entities
    ##################################################################

    def apply_schema_types_to_row(self, row_dict, schema):
        for field in schema.fields:
            if field.name in row_dict:
                row_dict[field.name] = field.type(row_dict[field.name])
        return row_dict

    def get_de_normalised_data_points(self, entity, parent, parent_dataset):
        # Get denormalised points - note that for scd this will pick randomly
        parent_fields = entity.schema.get_fields_for_parent(parent)
        parent_data = {str(f): random.choice(parent_dataset)[str(f)] for f in parent_fields}
        return parent_data

    def generate_entity_data(self, entity, datasets, num_iterations, print_progress):
        milestones = [int(i * num_iterations / NUM_DOTS) for i in range(1, NUM_DOTS + 1)]

        ents = {}
        relation_id_lists = {rel.name: list(datasets[rel.name].keys()) for rel in entity.relations}

        one_to_ones = {}
        for relation in entity.one_to_many_relations:  # Same per fact per instance, but uniquely sampled
            if relation.unique:
                one_to_ones[relation.name] = random.sample(relation_id_lists[relation.name], num_iterations)

        for i in range(num_iterations):
            for mile in milestones:
                if mile == i and print_progress:
                    print(".".format(entity.name), end="", flush=True)

            base = {}
            for relation in entity.one_to_many_relations:  # These will be the same per fact per instance
                if relation.unique:
                    rel_id = one_to_ones[relation.name][i]
                else:
                    rel_id = random.sample(relation_id_lists[relation.name], 1)[0]  # Not unique
                rel_id_name = self.entity_dict[relation.name].id
                base[rel_id_name] = rel_id
                base.update(self.get_de_normalised_data_points(entity, relation.name, datasets[relation.name][rel_id]))

            num_facts = entity.num_entities_per_iteration
            many_to_many_ids = {}
            for rel in entity.many_to_many_relations:  # These will be the same per fact
                if rel.unique:
                    many_to_many_ids[rel.name] = random.sample(relation_id_lists[rel.name], num_facts)
                else:
                    many_to_many_ids[rel.name] = [random.sample(relation_id_lists[rel.name], 1)[0]
                                                  for i in range(num_facts)]
            uid = None
            for j in range(num_facts):
                if not (uid and entity.preserve_id_across_its):  # Only make once if SCD Type 2
                    while True:  # Get a unique id for this instance
                        uid = str(uuid.uuid4())[-12:]  # 36 ** 12 is max num entities...
                        if uid not in ents:
                            ents[uid] = []
                            break
                inst = {entity.id: uid}
                inst.update(base)

                for rel in entity.many_to_many_relations:
                    rel_id = many_to_many_ids[rel.name].pop(0)
                    rel_id_name = self.entity_dict[rel.name].id
                    inst[rel_id_name] = rel_id
                    inst.update(self.get_de_normalised_data_points(entity, rel.name, datasets[rel.name][rel_id]))

                inst.update(entity.generate_entity(datasets, **inst))
                inst = self.apply_schema_types_to_row(inst, entity.schema)
                ents[uid].append(inst)

        if print_progress:
            print(" DONE")
        return ents

    ##################################################################
    # Save File
    ##################################################################
    @staticmethod
    def create_path(path):
        if path and not os.path.exists(path):
            os.makedirs(path)

    def to_csv(self, path=''):
        self.create_path(path)

        for name, uids in self.datasets.items():
            with open(os.path.join(path, name + ".csv"), "w+") as f:
                wr = csv.writer(f)
                headers = True
                for rows in uids.values():
                    for row in rows:
                        if headers:
                            headers = False
                            wr.writerow(list(row.keys()))

                        wr.writerow(list(row.values()))

    def to_json(self, path=''):
        self.create_path(path)

        for name, uids in self.datasets.items():
            with open(os.path.join(path, name + ".json"), "w+") as f:
                json.dump([val for val in uids.values()], f, default=json_serial)

    def to_pickled_pyschema(self, path=''):
        self.create_path(path)

        # Get first row
        for name, uids in self.datasets.items():
            with open(os.path.join(path, name + ".schema"), "wb") as f:
                first_row = next(iter(uids.values()))[0]
                schema = {}
                for name, val in first_row.items():
                    schema[name] = type(val)
                pickle.dump(schema, f)

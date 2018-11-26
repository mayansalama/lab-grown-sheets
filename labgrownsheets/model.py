import random
import uuid
from typing import Dict

import networkx

from labgrownsheets.profilers import *
from labgrownsheets.mixins.model.save_datasets import SaveDatasetsMixin

NUM_DOTS = 20


class StarSchemaModel(SaveDatasetsMixin):

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
        return StarSchemaModel([str_to_class(val[0]).init_handler(val[1]) for val in l])

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
        parent_fields = entity.schema.get_fields_for_parent(parent)
        parent_data = {str(f): parent_dataset[str(f)] for f in parent_fields}
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

            for j in range(num_facts):
                while True:  # Get a unique id for this instance
                    uid = str(uuid.uuid4())[-12:]  # 36 ** 12 is max num entities...
                    if uid not in ents:
                        break
                inst = {entity.id: uid}
                inst.update(base)

                for rel in entity.many_to_many_relations:
                    rel_id = many_to_many_ids[rel.name].pop(0)
                    rel_id_name = self.entity_dict[rel.name].id
                    inst[rel_id_name] = rel_id
                    inst.update(self.get_de_normalised_data_points(entity, rel.name, datasets[rel.name][rel_id]))

                inst.update(entity.generate_entity())
                inst = self.apply_schema_types_to_row(inst, entity.schema)
                ents[uid] = inst

        if print_progress:
            print(" DONE")
        return ents

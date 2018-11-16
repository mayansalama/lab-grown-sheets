import os
import uuid
import random
import csv
import json
import pickle
import inspect
from datetime import datetime, date
from typing import Dict

import networkx


NUM_DOTS = 20


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


class Relation:
    def __init__(self, name, type=None, unique=False):
        self.name = name
        self.type = type or "one_to_many"
        self.unique = bool(unique)

    @staticmethod
    def from_dict(dict):
        return Relation(dict['name'],
                        dict.get('type'),
                        dict.get("unique"))

    @property
    def id(self):
        return self.name + "_id"


class Entity:
    def __init__(self, name, generator_function, num_iterations, relations, num_facts_per_iter=None):
        self.name = name
        self.gen = generator_function
        self.num_iterations = num_iterations
        self.relations = relations
        if not num_facts_per_iter:
            num_facts_per_iter = 1
        self.num_facts_per_iter = num_facts_per_iter

    @staticmethod
    def from_dict(dict):
        name = dict['name']
        gen = dict['generator_function']
        num_iterations = dict['num_iterations']
        relations = dict.get('relations', [])
        num_facts_per_iter = dict.get('num_facts_per_iter')

        return Entity(name, gen, num_iterations, relations, num_facts_per_iter)

    @property
    def id(self):
        return self.name + "_id"

    @property
    def relations(self):
        return self._relations

    @relations.setter
    def relations(self, relations):
        if relations and isinstance(relations, list):
            self._relations = [Relation.from_dict(rel) if isinstance(rel, dict) else rel for rel in relations]
        else:
            self._relations = []

    @property
    def one_to_many_relations(self):
        return [rel for rel in self.relations if rel.type == "one_to_many"]

    @property
    def many_to_many_relations(self):
        return [rel for rel in self.relations if rel.type == "many_to_many"]

    ############################################################################
    # Generator Handlers
    ############################################################################

    @staticmethod
    def _check_if_gen(val):
        test = val()
        if inspect.isgenerator(test):
            return test
        else:
            return val

    @staticmethod
    def _return_executable(val):
        if inspect.isgenerator(val):
            return lambda: next(val)
        return val

    @property
    def gen(self):
        return self._return_executable(self._gen)

    @gen.setter
    def gen(self, val):
        self._gen = self._check_if_gen(val)

    @property
    def num_facts_per_iter(self):
        return self._return_executable(self._num_facts_per_iter)

    @num_facts_per_iter.setter
    def num_facts_per_iter(self, val):
        if not callable(val):
            if str(val).isnumeric():
                val = int(float(str(val)))
                func = lambda: val
            else:
                raise ValueError("Num facts per iteration must be either numeric or a function")
        else:
            func = self._check_if_gen(val)

        if not isinstance(func(), int):
            raise ValueError("Num facts per iteration must return an integer")
        self._num_facts_per_iter = func


class DummyStarSchema:

    ##################################################################
    # Init and props
    ##################################################################

    def __init__(self, entity_list):
        self.entity_dict: Dict[Entity] = {
            entity.name: entity for entity in entity_list
        }
        self.dag = self.generate_dag()
        self.datasets = None
        self.generate()

    @staticmethod
    def initiate_from_entity_list(list):
        return DummyStarSchema([
            Entity.from_dict(d) for d in list
        ])

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

    ##################################################################
    # Create Entities
    ##################################################################

    def generate_entity_data(self, entity, datasets):
        milestones = [int(i * entity.num_iterations / NUM_DOTS) for i in range(1, NUM_DOTS + 1)]

        ents = {}
        relation_id_lists = {rel.id: list(datasets[rel.name].keys()) for rel in entity.relations}

        one_to_ones = {}
        for relation in entity.one_to_many_relations:  # Same per fact per instance, but uniquely sampled
            if relation.unique:
                one_to_ones[relation.id] = random.sample(relation_id_lists[relation.id], entity.num_iterations)

        for i in range(entity.num_iterations):
            for k in [mile for mile in milestones if mile == i]:
                print(".".format(entity.name), end="", flush=True)

            base = {}
            for relation in entity.one_to_many_relations:  # These will be the same per fact per instance
                if relation.unique:
                    base[relation.id] = one_to_ones[relation.id][i]
                else:
                    base[relation.id] = random.sample(relation_id_lists[relation.id], 1)[0]  # Not unique

            num_facts = entity.num_facts_per_iter()  # Defaults to uniform 1
            many_to_many_ids = {}
            for rel in entity.many_to_many_relations:  # These will be the same per fact
                if rel.unique:
                    many_to_many_ids[rel.id] = random.sample(relation_id_lists[rel.id], num_facts)
                else:
                    many_to_many_ids[rel.id] = [random.sample(relation_id_lists[rel.id], 1)[0]
                                                for i in range(num_facts)]

            for j in range(num_facts):
                while True:  # Get a unique id for this instance
                    uid = str(uuid.uuid4())[-12:]  # 36 ** 12 is max num entities...
                    if uid not in ents:
                        break
                inst = {entity.id: uid}
                inst.update(base)

                for relation in entity.many_to_many_relations:
                    inst[relation.id] = many_to_many_ids[relation.id].pop(0)

                inst.update(entity.gen())
                ents[uid] = inst

        print(" DONE")
        return ents

    def generate(self):
        datasets = {}
        max_name_length = len(max(self.entity_dict.keys(), key=len))

        for entity in networkx.topological_sort(self.dag):
            print("Generating entity {}{}  ".format(entity.name, ' ' * (max_name_length - len(entity.name))),
                  end="", flush=True)
            datasets[entity.name] = self.generate_entity_data(entity, datasets)

        self.datasets = datasets

    ##################################################################
    # Save File
    ##################################################################

    def to_csv(self, folder):
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        for name, uids in self.datasets.items():
            with open((folder + '/' if folder else '') + name + ".csv", "w+") as f:
                wr = csv.writer(f)
                headers = True
                for row in uids.values():
                    if headers:
                        headers = False
                        wr.writerow(list(row.keys()))

                    wr.writerow(list(row.values()))

    def to_json(self, folder):
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        for name, uids in self.datasets.items():
            with open((folder + '/' if folder else '') + name + ".json", "w+") as f:
                json.dump(list(uids.values()), f, default=json_serial)

    def to_schemas(self, folder):
        if folder and not os.path.exists(folder):
            os.makedirs(folder)

        for name, uids in self.datasets.items():
            with open((folder + '/' if folder else '') + name + ".schema", "wb") as f:
                first_row = next(iter(uids.values()))
                schema = {}
                for name, val in first_row.items():
                    schema[name] = type(val)
                pickle.dump(schema, f)

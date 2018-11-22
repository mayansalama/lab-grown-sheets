# LabGrownData
Data profilers for relational databases. Written in Python 3.7.

### Usage
A model consists of a collection of profilers, which have a 1:1 correspondence to a
table. A profiler is responsible for creating entities, and stores additional info
defining the tables relations (parent entities and denormalised columns) and schema.

See examples on how a model can be constructed to build a basic star schema data 
structure.
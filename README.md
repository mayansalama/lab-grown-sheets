# LabGrownData
Data profilers for relational databases. Written in Python 3.7. The 
purpose for building this was to develop test suites for Data reliant
tools. This works well with tools like dbt, an example of the integration
between the two can be seen [here.](https://github.com/mayansalama/dbt-sandbox)

### Usage
A model consists of a collection of profilers, which have a 1:1 correspondence to a
table. A profiler is responsible for creating entities, and stores additional info
defining the tables relations (parent entities and denormalised columns) and schema.

See examples for demonstrations on how a model can be constructed to build a basic star schema data 
structure.

### Testing
```
sh run_test.sh
```
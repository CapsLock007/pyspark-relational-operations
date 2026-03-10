# Relational Operations with MapReduce using PySpark

This repository contains implementations of relational algebra operations using the MapReduce paradigm with **PySpark**.

The goal of this project is to demonstrate how common SQL operations can be expressed and implemented using the **Map and Reduce model**.

## Implemented Operations

The following relational operations are implemented:

1. **Union**
2. **Intersection**
3. **Difference**
4. **Inner Join**
5. **Selection**
6. **Projection**
7. **Grouping and Aggregation**
8. **Outer Join (LEFT / RIGHT)**

Each operation is implemented in a separate Python script.

## Dataset

All scripts use the dataset:

data/person.csv


The dataset contains example information about people and is used to demonstrate relational operations.

## Project Structure

pyspark-relational-operations/
  data/
    person.csv
  tasks/
    union.py
    intersection.py
    difference.py
    inner_join.py
    selection.py
    projection.py
    grouping_aggregation.py
    outer_join.py
  README.md

## Running the Code

Example command for running a task:
spark-submit tasks/union.py data/person.csv


Make sure **Apache Spark** is installed and available in your environment.

## Technologies

This project uses:

- Python
- PySpark
- MapReduce paradigm
- SQL relational operations

## Purpose

This repository was created as part of coursework related to **data processing and parallel computing**.  
It demonstrates how relational database operations can be implemented using distributed data processing frameworks.

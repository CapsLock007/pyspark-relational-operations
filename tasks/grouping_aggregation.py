"""
Task 2.7 – GROUPING AND AGGREGATION

SQL equivalent:

SELECT a, function(b)
FROM A
GROUP BY a

Description:
Groups tuples by attribute A and performs
an aggregation function on attribute B.

Example relation:
R(A, B, C)

Grouping attribute: A
Aggregated attribute: B
Attribute C is ignored.

Map:
map(a, b, c) -> (a, b)

Reduce:
reduce(a, [b1, b2, ..., bn]) -> (a, function(b1, b2, ..., bn))
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-7.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("GroupingAggregation").getOrCreate()
    
    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())
    
    mapped = rdd.map(lambda d: (d["country"], d["income"].replace("$", "")))
    grouped = mapped.groupByKey()
    # result = grouped.map(lambda kv: (kv[0], #suma income dla poszczególnych krajów
    #                                  sum(float(x) for x in kv[1])))
    result = grouped.map(lambda kv: (kv[0], #średnia income dla poszczególnych krajów
                                     sum(float(x) for x in kv[1]) / len(list(kv[1]))))
    # result = grouped.map(lambda kv: (kv[0], #max income dla poszczególnych krajów
    #                                  max(float(x) for x in kv[1])))
    if not result.collect():
        print("Brak elementów")
    else:
        for i in result.collect():
            print(i)
            
        spark.stop()

if __name__ == "__main__":
    main()
"""
Task 2.2 – INTERSECTION

SQL equivalent:

SELECT * FROM A
INTERSECT
SELECT * FROM B

Description:
Returns only tuples that appear in both relations A and B.

Map:
map(t) -> (t, t)

Reduce:
If for a given key we obtain values [t, t] after sorting,
output (t, t). Otherwise nothing is emitted.
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-2.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("Intersection").getOrCreate()

    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())

    #A = rdd.filter(lambda d: d['country'] == "Indonesia")
    A = rdd.filter(lambda d: int(d['id']) < 20) #id 1-19
    A_mapped = A.map(lambda t: (tuple(sorted(t.items())), t))
    B = rdd.filter(lambda d: int(d['id']) < 8) #id 1-7
    #B = rdd.filter(lambda d: d['country'] == "Canada")
    B_mapped = B.map(lambda t: (tuple(sorted(t.items())), t))

    combined = A_mapped.union(B_mapped)
    grouped = combined.groupByKey()
    intersection = grouped.filter(lambda kv: len(list(kv[1])) == 2)
    result = intersection.map(lambda kv: kv[1]).map(lambda vals: list(vals)[0])

    if not result.collect():
        print("Brak elementów wspólnych")
    else:
        for i in result.collect():
            print(i)

    spark.stop()

if __name__ == "__main__":
    main()
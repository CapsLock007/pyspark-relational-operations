"""
Task 2.3 – DIFFERENCE

SQL equivalent:

SELECT * FROM A
EXCEPT
SELECT * FROM B

Description:
Returns tuples that appear in relation A but not in relation B.

Map:
map(t) -> (t, R)
where R denotes the relation identifier (A or B).

Reduce:
If for a given key we obtain value [R] after sorting,
output (t, t). Otherwise nothing is emitted.
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-3.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("Difference").getOrCreate()

    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())
    
    A = rdd.filter(lambda d: int(d['id']) < 20) #id 1-19
    A_mapped = A.map(lambda t: (tuple(sorted(t.items())), "A"))
    B = rdd.filter(lambda d: int(d['id']) < 8) #id 1-7
    B_mapped = B.map(lambda t: (tuple(sorted(t.items())), "B"))
    
    combined = A_mapped.union(B_mapped)
    grouped = combined.groupByKey()
    diff = grouped.filter(lambda kv: sorted(list(kv[1])) == ["A"])
    result = diff.map(lambda kv: dict(kv[0]))
    
    if not result.collect():
        print("Brak elementów")
    else:
        for i in result.collect():
            print(i)

    spark.stop()

if __name__ == "__main__":
    main()
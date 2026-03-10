"""
Task 2.6 – PROJECTION

SQL equivalent:

SELECT x, y, z FROM A

Description:
Outputs only selected attributes from each tuple.

Map:
map(t) -> (t, t')

where t' is created from tuple t by keeping only
attributes specified in the projection.

Reduce:
reduce(t, [t', t', ..., t']) -> (t, t')
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-6.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("Projection").getOrCreate()

    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())
    
    mapped = rdd.map(
        lambda t: (
            tuple(sorted(t.items())),
            {col: t[col] for col in ["id", "country", "income"]}))
    result = mapped.groupByKey().map(lambda kv: list(kv[1])[0])
    
    if not result.collect():
        print("Brak elementów")
    else:
        for i in result.collect():
            print(i)

    spark.stop()

if __name__ == "__main__":
    main()
"""
Task 2.1 – UNION

SQL equivalent:

SELECT * FROM A
UNION
SELECT * FROM B

Description:
Returns all unique tuples that appear in either relation A or relation B.

Map:
map(t) -> (t, t)

Reduce:
reduce(t, [t, t, ..., t]) -> (t, t)
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-1.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("Union").getOrCreate()

    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())
    """
    def union(A, B):
        A_tuples = A.map(lambda d: tuple(d.items()))
        B_tuples = B.map(lambda d: tuple(d.items()))

        combined = A_tuples.collect() + B_tuples.collect()
        unique = list(set(combined))
        result = A.context.parallelize([dict(t) for t in unique])
        return result
    """
    #union osób z Indonezji i Kanady
    A = rdd.filter(lambda d: d['country'] == "Indonesia")
    B = rdd.filter(lambda d: d['country'] == "Canada")
    
    #union wbudowane
    A_mapped = A.map(lambda t: (tuple(t.values()), t))
    B_mapped = B.map(lambda t: (tuple(t.values()), t))
    combined = A_mapped.union(B_mapped)
    grouped = combined.reduceByKey(lambda a, b: a)
    result = grouped.map(lambda kv: kv[0])
    
    #result = union(A, B)
    result2 = result.sortBy(lambda d: (0 if d['country'] == 'Canada' else 1, int(d['id']))) #Kanada na przodzie i sortuję po id
    #for i in result.collect():
    for i in result2.take(20): #żeby mieć wszystkich: for i in result2.collect():
        print(i)

    spark.stop()

if __name__ == "__main__":
    main()
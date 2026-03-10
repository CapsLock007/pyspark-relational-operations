"""
Task 2.5 – SELECTION

SQL equivalent:

SELECT * FROM A WHERE condition

Description:
Filters tuples that satisfy a given condition.

Map:
For each tuple t, check if it satisfies the selection condition.
If yes, emit (t, t).

Reduce:
Identity function.
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-5.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("Selection").getOrCreate()

    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())
    
    # mapped = rdd.map(lambda t: (tuple(sorted(t.items())), t) #państwo Kanada
    #                  if t["country"] == "Canada" else None)
    # mapped = rdd.map(lambda t: ((tuple(sorted(t.items()))), t) #dochód powyżej 500k
    #                  if float(t["income"].replace("$", "").replace(",", "")) > 500000 else None)
    mapped = rdd.map(lambda t: ((tuple(sorted(t.items()))), t) #dochód 100-200k
                     if 100000 <= float(t["income"].replace("$", "").replace(",", "")) <= 200000 else None)
    filtered = mapped.filter(lambda x: x is not None)
    result = filtered.groupByKey().map(lambda kv: list(kv[1])[0])
    
    if not result.collect():
        print("Brak elementów")
    else:
        for i in result.collect():
            print(i)

    spark.stop()

if __name__ == "__main__":
    main()
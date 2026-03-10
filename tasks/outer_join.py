"""
Task 2.8 – OUTER JOIN

SQL equivalent:

LEFT OUTER JOIN or RIGHT OUTER JOIN

Description:
Extends the inner join operation by preserving tuples
from one relation even if no matching tuple exists
in the other relation.

LEFT OUTER JOIN:
All tuples from the left relation are preserved.

RIGHT OUTER JOIN:
All tuples from the right relation are preserved.

Implementation is based on the previously implemented join logic
with additional handling of unmatched tuples.
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-8.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("OuterJoins").getOrCreate()

    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())

    # R has (country, id)
    # S has (country, income)
    R = rdd.map(lambda d: (d['country'], ("R", d['id'])))
    S = rdd.map(lambda d: (d['country'], ("S", d['income'])))

    combined = R.union(S)
    grouped = combined.groupByKey()
    
    def left_join(kv):
        key = kv[0]
        values = list(kv[1])

        R_vals = [v[1] for v in values if v[0] == "R"]
        S_vals = [v[1] for v in values if v[0] == "S"]

        if S_vals:
            #join
            return [
                {"id": r, "country": key, "income": s}
                for r in R_vals
                for s in S_vals]
        else:
            #brak dopasowania S
            return [
                {"id": r, "country": key, "income": None}
                for r in R_vals]
    left = grouped.flatMap(left_join)
    
    def right_join(kv):
        key = kv[0]
        values = list(kv[1])

        R_vals = [v[1] for v in values if v[0] == "R"]
        S_vals = [v[1] for v in values if v[0] == "S"]

        if R_vals:
            #join
            return [
                {"id": r, "country": key, "income": s}
                for r in R_vals
                for s in S_vals]
        else:
            #brak dopasowania R
            return [
                {"id": None, "country": key, "income": s}
                for s in S_vals]
    right = grouped.flatMap(right_join)
    
    for i in left.collect():
        print(i)
    for j in right.collect():
        print(j)
        
    spark.stop()
    
if __name__ == "__main__":
    main()
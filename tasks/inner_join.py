"""
Task 2.4 – INNER JOIN

SQL equivalent:

SELECT * FROM R, S WHERE R.B = S.B

Description:
Performs an inner join between relations R(A, B) and S(B, C)
on the common attribute B.

Map:
map(a, b) -> (b, (R, a))
map(b, c) -> (b, (S, c))

Reduce:
reduce(b, [(R, a), (S, c)]) -> (b, (a, b, c))
"""
import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Użycie: spark-submit zad2-4.py person.csv")
        sys.exit(1)

    path = sys.argv[1]
    spark = SparkSession.builder.appName("InnerJoin").getOrCreate()

    df = spark.read.option("header", True).csv(path)
    rdd = df.rdd.map(lambda row: row.asDict())
    
    R = rdd.map(lambda d: (d['country'], ("R", d['id'])))
    S = rdd.map(lambda d: (d['country'], ("S", d['income'])))
    
    combined = R.union(S)
    grouped = combined.groupByKey()
    
    def reduce_join(kv):
        key = kv[0]          #country
        values = list(kv[1]) #np. [("R","17"), ("R","22"), ("S","$100000"), ...]

        R_values = [v[1] for v in values if v[0] == "R"]
        S_values = [v[1] for v in values if v[0] == "S"]

        #lista (id, country, income)
        result = []
        for a in R_values:
            for c in S_values:
                result.append({"id": a, "country": key, "income": c})
        return result
    
    result = grouped.flatMap(reduce_join)
    """
    result = grouped.map(lambda kv: [
            {"id": a, "country": kv[0], "income": c}
            for (tag1, a) in kv[1] if tag1 == "R"
            for (tag2, c) in kv[1] if tag2 == "S"
        ])
    lista = []
    for i in result.collect():
        for row in i:
            lista.append(row)
            
    if not lista:
        print("Brak elementów")
    else:
        for j in lista:
            print(j)
    """
    if not result.collect():
        print("Brak elementów")
    else:
        for j in result.collect():
            print(j)
         
    spark.stop()

if __name__ == "__main__":
    main()
    
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat


def run():
    spark = SparkSession \
        .builder \
        .appName("spark demo example") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.json("../../../resources/students.json")
    df1 = df.withColumn("fullname", concat(col("firstname"), lit(' '), col("lastname"))).drop("firstname", "lastname")
    df2 = df1.select("fullname", "department", "english", "maths", "science")
    df2.show(50, truncate=False)


if __name__ == '__main__':
    run()

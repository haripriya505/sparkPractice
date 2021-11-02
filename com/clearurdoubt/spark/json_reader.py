from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, round


def run():
    spark = SparkSession \
        .builder \
        .appName("spark demo example") \
        .master("local[*]") \
        .enableHiveSupport() \
        .getOrCreate()

    df = spark.read.json("../../../resources/students.json")
    df1 = df.withColumn("fullname", concat(col("firstname"), lit(' '), col("lastname"))).drop("firstname", "lastname") \
        .withColumn("total", col("english") + col("maths") + col("science")) \
        .withColumn("percentage", round(((col("english") + col("maths") + col("science")) / 300)*100, 2)) \
        .withColumn("grade",
                    when((col("english") < 35) | (col("maths") < 35) | (col("science") < 35), lit("fail")).otherwise(
                        when((col("english") > 35) & (col("maths") > 35) & (col("science") > 35) & (col("total") > 210),
                             lit("distinction")).otherwise(
                            when(((col("english") > 35) & (col("maths") > 35) | (col("science") > 35)) & (
                                        col("total") > 180),
                                 lit("first class")).otherwise(lit("Pass"))))
                    )
    df2 = df1.select("fullname", "department", "english", "maths", "science", "percentage" , "total", "grade")
    df2.printSchema()
    df2.show(50, truncate=False)


if __name__ == '__main__':
    run()

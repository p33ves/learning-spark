import pyspark
from operator import add

sc = pyspark.SparkContext('local[*]')
spark = pyspark.sql.SparkSession \
    .builder \
    .appName("TestApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
netflix_data = spark.read.csv('netflix_data.csv', header=True).rdd
desc = netflix_data.map(lambda x: x[5])
# print(desc)
desc.saveAsTextFile('descriptions.txt')

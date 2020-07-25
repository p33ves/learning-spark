import pyspark
from operator import add

spark = pyspark.sql.SparkSession \
    .builder \
    .appName("TestApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
netflix_data = spark.read.csv('netflix_data.csv', header=True).rdd
desc = netflix_data.map(lambda x: x[5])
words = desc.flatMap(lambda line: str(line).split(' '))
word_count = words.map(lambda word: (word, 1)).reduceByKey(add)
output = word_count.collect()
with open('output.txt', 'w') as f:
    for word, count in output:
        f.write(f"{word} : {count}\n")

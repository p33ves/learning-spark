import pyspark
from operator import add


def word_cleaner(word: str) -> str:
    new_word = word
    spl_chars = set([',', '.', ':', '"', "'", '”', '“', ')', '(', '?', '…'])
    word_chars = set(new_word)
    for char in word_chars.intersection(spl_chars):
        new_word = new_word.replace(char, '')
    return new_word.lower()


spark = pyspark.sql.SparkSession \
    .builder \
    .appName("TestApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
netflix_data = spark.read.csv('netflix_data.csv', header=True).rdd
desc = netflix_data.map(lambda x: x[5])
words = desc.flatMap(lambda line: str(line).split(' '))
word_count = words.map(lambda word: (word_cleaner(word), 1)).reduceByKey(add)
# word[:-1], 1) if word[-1] in (',', '.', ':') and len(word[:-1]) else (word, 1))
#word_count = words.map(lambda word: (word, 1)).reduceByKey(add)
output = word_count.collect()
with open('output.txt', 'w', encoding="utf-8") as f:
    for word, count in output:
        f.write(f"{word} : {count}\n")

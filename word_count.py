from pyspark import SparkContext
from operator import add

sc = SparkContext(appName="TestApp")
desc_text = sc.textFile('descriptions.txt')
words = desc_text.flatMap(lambda line: line.split(' '))
word_count = words.map(lambda word: (word, 1)).reduceByKey(add)
output = word_count.collect()
with open('output.txt', 'w') as f:
    for word, count in output:
        f.write(f"{word} : {count}\n")

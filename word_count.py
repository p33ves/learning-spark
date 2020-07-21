from pyspark import SparkContext

sc = SparkContext(appName="TestApp")
desc_text = sc.textFile('descriptions.txt')
words = desc_text.flatMap(lambda line: line.split(' '))
word_count = words.map(lambda x: (x, 1))
word_count.reduceByKey(lambda x, y: x+y).collect()
print(vars(word_count))

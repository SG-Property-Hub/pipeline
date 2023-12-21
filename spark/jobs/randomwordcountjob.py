from pyspark.sql import SparkSession
import random
import string

spark = SparkSession.builder.appName("RandomDataWordCount").getOrCreate()
data = []
for _ in range(1000):
    random_word = ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 10)))
    data.append(random_word)

words = spark.sparkContext.parallelize(data)
word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

for wc in word_counts.collect():
    print(wc[0], wc[1])

spark.stop()

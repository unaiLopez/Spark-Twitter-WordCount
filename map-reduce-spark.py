from __future__ import print_function
from pyspark import SparkContext
from pyspark.conf import SparkConf
import json
import re
from operator import add

def mapper(line):
	try:
		lineJson = json.loads(line)
		text = re.sub(r'[^\w]', ' ', lineJson["text"]).lower()
		return text
	except:
		return ""

if __name__ == "__main__":
	conf = SparkConf()
	conf.setMaster("local[4]")
	conf.setAppName("WordCount Coronavirus English Tweets")
	sc = SparkContext(conf=conf)

	tweetsRDD = sc.textFile("sample_tweets.txt")
	tweetsCleanedRDD = tweetsRDD.map(lambda line: mapper(line)).filter(lambda x: x != "")
	tweetsMappedRDD = tweetsCleanedRDD.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
	tweetsWordCountRDD = tweetsMappedRDD.reduceByKey(add)

	tweetsWordCountRDD.saveAsTextFile("output/")

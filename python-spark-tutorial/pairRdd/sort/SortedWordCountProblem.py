from pyspark import SparkContext, SparkConf
import sys
sys.path.insert(0, '.')


if __name__ == "__main__":
	'''
    Create a Spark program to read the an article from in/word_count.text,
    output the number of occurrence of each word in descending order.

    Sample output:

    apple : 200
    shoes : 193
    bag : 176
    ...

    '''
	conf = SparkConf().setAppName("word count").setMaster("local[*]")
	sc = SparkContext(conf = conf)

	lines = sc.textFile("in/word_count.text")

	words = lines.flatMap(lambda line: line.split(" "))
	wordPairRdd = words.map(lambda word : (word, 1))

	wordCount = words.reduceByKey(lambda x, y: x + y)
	sortedCount = wordCount.sortBy(lambda wordCount: wordCount[1], ascending=False)

	for word, cnt in sortedCount.collect():
		print("{} : {}".format(word, cnt))


    

import sys
from pyspark import SparkContext

if __name__ == "__main__":

    '''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
    '''
    conf = SparkConf().setAppName("prime sum").setMaster("local[*]")
    sc = SparkContext(conf = conf)
   
    lines = sc.textFile("in/prime_nums.text")
    nums = lines.flatMap(lambda line: line.split("\t"))   
   	validNum = nums.filter(lambda number: number)
   	intNum = validNum.map(lambda number: int(number))
    prime_sum = validNum.reduce(lambda x, y: x + y)

    print("sum is :{}".format(prime_sum))


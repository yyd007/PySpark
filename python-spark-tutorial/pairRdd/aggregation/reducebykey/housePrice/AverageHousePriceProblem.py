from pyspark import SparkContext, SparkConf
import sys
sys.path.insert(0,'.')
from pairRdd.aggregation.reducebykey.housePrice.AvgCount import AvgCount



if __name__ == "__main__":

    conf = SparkConf().setAppName("RealEstate").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("in/RealEstate.csv")

    # remove header line
    cleanedLines = lines.filter(lambda line: " Bedrooms" not in line)

    housePricePairRdd = cleanedLines.map(lambda line: line.split(",")[3], AvgCount(1,float(line.split(",")[2])))
    housePriceTotal = housePricePairRdd.reduceByKey(lambda x, y: AvgCount(x.count + y.count, x.total + y.total))
    
    print("housePriceTotal: ")
    for bedroom, avgCount in housePriceTotal.collect():
        print("{} : ({}, {})".format(bedroom, avgCount.count, avgCount.total))

    housePriceAvg = housePriceTotal.mapValues(lambda avgCount: avgCount.total / avgCount.count)

    print("\nhousePriceAvg: ")
    for bedroom, avg in housePriceAvg.collect():
        print("{} : ({}, {})".format(bedroom, avg))

    # realEstate = realEstatePairRDD.filter(lambda keyValue: keyValue[1] != "\"United States\"")

    # airportsNotInUSA.saveAsTextFile("out/airports_not_in_usa_pair_rdd.text")

# wordRdd = lines.flatMap(lambda line: line.split(" "))
    # wordPairRdd = wordRdd.map(lambda word: (word, 1))

    # wordCounts = wordPairRdd.reduceByKey(lambda x, y: x + y)
    # for word, count in wordCounts.collect():
        # print("{} : {}".format(word, count))

    '''
    Create a Spark program to read the house data from in/RealEstate.csv,
    output the average price for houses with different number of bedrooms.

    The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
    around it. 

    The dataset contains the following fields:
    1. MLS: Multiple listing service number for the house (unique ID).
    2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
    northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
    some out of area locations as well.
    3. Price: the most recent listing price of the house (in dollars).
    4. Bedrooms: number of bedrooms.
    5. Bathrooms: number of bathrooms.
    6. Size: size of the house in square feet.
    7. Price/SQ.ft: price of the house per square foot.
    8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

    Each field is comma separated.

    Sample output:

       (3, 325000)
       (1, 266356)
       (2, 325000)
       ...

    3, 1 and 2 mean the number of bedrooms. 325000 means the average price of houses with 3 bedrooms is 325000.

    '''

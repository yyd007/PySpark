from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("create").setMaster("local")
    sc = SparkContext(conf = conf)

    tuples = [("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8)]
    pairRDD = sc.parallelize(tuples)
    # get back pair rdd object

    # coalesce - reduce rdd to 1 partition - assures we only have 1 file 
    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")

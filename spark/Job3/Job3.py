"""spark application"""
import argparse
from pyspark.sql import SparkSession
import re
import itertools

regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"


def filter_unique_reverse(x):
    if x[0][0] != x[1][0]:
        return True
    if "\n" not in x[1][0]:
        return True
    return False


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# parse arguments
args = parser.parse_args()
input_filepath, output_filepath = args.input_path, args.output_path

# initialize SparkSession with the proper configuration
spark = SparkSession \
    .builder \
    .appName("Job1 Spark") \
    .config("spark.executor.instances", 15) \
    .getOrCreate()

# read the input file and obtain an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath).cache()

# remove csv header
filtered_lines_RDD = lines_RDD.filter(f=lambda word: not word.startswith("Id") and not word.endswith("Text"))

more_than_4_product_score_RDD = filtered_lines_RDD.filter(f=lambda line: int(re.split(regex, line)[6]) >= 4)

user_2_products_RDD = more_than_4_product_score_RDD. \
    map(f=lambda line: (re.split(regex, line)[2], re.split(regex, line)[1]))

user_2_products_reduced_RDD = user_2_products_RDD.reduceByKey(func=lambda a, b: a + " " + b)

at_least_3_products_RDD = user_2_products_reduced_RDD.filter(f=lambda line: len(line[1].split(" ")) >= 3).\
    map(f=lambda x: (x[0], set(x[1].split(" "))))

user_2_product_dictionary = at_least_3_products_RDD.collectAsMap()


# create a generator of tuple removing the reversed tuple ( if I have (1,2) i don't want (2,1) and same value (1,1)
# also removing the tuples that have less than 3 products after intersecting their product list
list_of_tuples = (tuple(i) for i in itertools.product(tuple(user_2_product_dictionary.keys()), repeat=2)
                  if tuple(reversed(i)) >= tuple(i) and i[0] != i[1]
                  and len(user_2_product_dictionary[i[0]].intersection(user_2_product_dictionary[i[1]])) >= 3)

sorted_final_RDD = spark.sparkContext.parallelize(sorted(list_of_tuples, key=lambda x: x)).\
    map(f=lambda x: (x, user_2_product_dictionary[x[0]].intersection(user_2_product_dictionary[x[1]]))).\
    filter(f=lambda x: len(x[1]) >= 3)

collapsed_RDD = sorted_final_RDD.coalesce(1)
collapsed_RDD.saveAsTextFile(output_filepath)

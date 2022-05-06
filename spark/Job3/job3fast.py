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

product_2_userID_RDD = more_than_4_product_score_RDD. \
    map(f=lambda line: (re.split(regex, line)[1], re.split(regex, line)[2]))

userID_2_product_RDD = more_than_4_product_score_RDD. \
    map(f=lambda line: (re.split(regex, line)[2], re.split(regex, line)[1]))

product_2_userID_reduced_RDD = product_2_userID_RDD.reduceByKey(func=lambda a, b: a + " " + b)

userID_2_product_reduced_RDD = userID_2_product_RDD.reduceByKey(func=lambda a, b: a + " " + b)

filtered_product_2_userID_reduced_RDD = product_2_userID_reduced_RDD.filter(f=lambda x: len(x[1].split(" ")) > 1). \
    map(f=lambda x: (x[0], x[1].split(" ")))

filtered_userID_2_product_reduced_RDD = userID_2_product_reduced_RDD.map(f=lambda x: (x[0], set(x[1].split(" "))))

product_2_userID_dictionary = filtered_product_2_userID_reduced_RDD.collectAsMap()
userID_2_product_dictionary = filtered_userID_2_product_reduced_RDD.collectAsMap()

final_dict = {}

for product in product_2_userID_dictionary.keys():
    pairs = list(itertools.combinations(product_2_userID_dictionary[product], 2))

    for pair in pairs:
        if pair and tuple(reversed(pair)) not in final_dict:
            products_intersection = userID_2_product_dictionary[pair[0]].\
                intersection(userID_2_product_dictionary[pair[1]])
            if len(products_intersection) >= 3:
                final_dict[pair] = products_intersection



final_RDD= spark.sparkContext.parallelize(sorted(final_dict.items() , key= lambda x: (x[0],x[1])))

# without_reverse_ordered_RDD = spark.sparkContext. \
#    parallelize(sorted(filtered_product_intersect_RDD.collect(), key=lambda x: x[0]))
collapsed_RDD = final_RDD.coalesce(1)
collapsed_RDD.saveAsTextFile(output_filepath)

"""spark application"""
import argparse
from pyspark.sql import SparkSession
import re
from operator import add

regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

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

# at_least_3_products_RDD = user_2_products_RDD.filter(f=lambda x: len(x[1]) >= 3)

user_2_products_reduced_RDD = user_2_products_RDD.reduceByKey(func=lambda a, b: a + " " + b)

# users_2_product_list_RDD = user_2_products_reduced_RDD.map(f=lambda x: (x[0], x[1].split(" ")))

at_least_3_products_RDD = user_2_products_reduced_RDD.filter(f=lambda line: len(line[1].split(" ")) >= 3)

simplified_RDD = at_least_3_products_RDD.map(f=lambda x: x[0] + "\t" + x[1])
cartesian_RDD = simplified_RDD.cartesian(simplified_RDD)

simplified_cartesian_2_pairs_RDD = cartesian_RDD.map(f=lambda x: (x[0].split("\t"), x[1].split("\t")))
filtered_cartesian_2_pairs_RDD = simplified_cartesian_2_pairs_RDD.filter(f=lambda x: x[0][0] != x[1][0])

product_intersect_RDD = filtered_cartesian_2_pairs_RDD.\
    map(f=lambda x: ((x[0][0], x[1][0]), set(x[0][1].split(" ")).intersection(set(x[1][1].split(" ")))))

filtered_product_intersect_RDD = product_intersect_RDD.\
    filter(f=lambda x: len(x[1]) >= 3)

collapsed_RDD = filtered_product_intersect_RDD.coalesce(1)
collapsed_RDD.saveAsTextFile(output_filepath)

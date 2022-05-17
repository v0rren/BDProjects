"""spark application"""
import argparse
from pyspark.sql import SparkSession
import re
import itertools


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

regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

# read the input file and obtain an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath).cache()

# remove csv header
filtered_lines_RDD = lines_RDD.filter(f=lambda word: not word.startswith("Id") and not word.endswith("Text"))

user_2_product_RDD = filtered_lines_RDD.map(f=lambda line: (re.split(regex, line)[2], re.split(regex, line)[1] + '\t' + re.split(regex, line)[6]))

user2_product_reduced_RDD = user_2_product_RDD.reduceByKey(func=lambda a, b: a + "\t\t" + b)

user_2_products_RDD = user2_product_reduced_RDD.map(f=lambda item: (item[0], item[1].split("\t\t"))).\
    map(f=lambda item: (item[0], [(x.split("\t")[0], x.split("\t")[1]) for x in item[1]]))

ordered_RDD = user_2_products_RDD.map(f=lambda x: (x[0], sorted(x[1], key=lambda item: item[1], reverse=True)))

top_five_RDD = ordered_RDD.map(f=lambda item: (item[0], item[1][:5])).coalesce(1)

to_order = top_five_RDD.collect()

final_RDD = spark.sparkContext.parallelize(sorted(to_order, key=lambda x: x[0])).coalesce(1)

final_RDD.saveAsTextFile(output_filepath)

#!/usr/bin/env python3

"""spark application"""
import argparse
from pyspark.sql import SparkSession
import re
from datetime import datetime
from collections import Counter

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
    .config("spark.executor.cores", 7) \
    .config("spark.default.parallelism", 15) \
    .config("spark.executor.memory", "30gb") \
    .getOrCreate()

# read the input file and obtain an RDD with a record for each line
lines_RDD = spark.sparkContext.textFile(input_filepath).cache()

# remove csv header
filtered_lines_RDD = lines_RDD.filter(f=lambda word: not word.startswith("Id") and not word.endswith("Text"))

stripped_lines_RDD = filtered_lines_RDD.map(f=lambda line: line.strip())

years_2_text_RDD = stripped_lines_RDD.map(f=lambda line: (datetime.fromtimestamp(int(re.split(regex, line)[7])).
                                                          strftime('%Y'), re.split(regex, line)[9]))

years_2_text_reduced_RDD = years_2_text_RDD.reduceByKey(func=lambda a, b: a + " " + b)

years_2_word_RDD = years_2_text_reduced_RDD.map(f=lambda item: (item[0], re.findall(r'[^,;\s]+', item[1])))

years_2_most_used_words_RDD = years_2_word_RDD.map(f=lambda item: (item[0], Counter(item[1]).most_common()))

years_2_top_10_most_used_words_RDD = years_2_most_used_words_RDD.map(f=lambda item: (item[0], item[1][:10]))

# write all <year, list of (word, occurrence)> pairs in file
years_2_top_10_most_used_words_RDD.coalesce(1).saveAsTextFile(output_filepath)

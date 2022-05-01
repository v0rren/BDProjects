#!/usr/bin/env python3
"""reducer.py"""

import sys
import re


# this dictionary maps each word to the sum of the values
# that the mapper has computed for that word
year_2_most_used_word = {}

# input comes from STDIN
# note: this is the output from the mapper!
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    current_date, current_string = line.split("\t\t")

    words = re.findall(r'[^,;\s]+', current_string)

    # initialize that were not seen before with empty dic
    if current_date not in year_2_most_used_word:
        year_2_most_used_word[current_date] = {}
    for word in words:

        if current_date in year_2_most_used_word and word in year_2_most_used_word[current_date]:
            year_2_most_used_word[current_date][word] += 1
        elif current_date in year_2_most_used_word:
            year_2_most_used_word[current_date][word] = 0

for year in year_2_most_used_word:
    sorted_words = sorted(year_2_most_used_word[year].items(), key=lambda x: x[1], reverse=True)
    print("%s\t%s" % (year, sorted_words[:10]))

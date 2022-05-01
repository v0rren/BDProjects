#!/usr/bin/env python3
"""mapper.py"""

import sys
import re

regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

infile = sys.stdin
next(infile)  # skip first line of input file

# read lines from STDIN (standard input)
for line in infile:
    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into rows using regex
    rows = re.split(regex, line)

    try:
        # if the product score is at least 4 we add the user id and productid
        if int(rows[6]) >= 4:
            userID = rows[2]
            productID = rows[1]

            print('%s\t\t%s' % (userID, productID))
    except ValueError:
        continue

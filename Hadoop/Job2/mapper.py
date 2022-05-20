#!/usr/bin/env python3
"""mapper.py"""

import sys
from datetime import datetime
import re
regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"

# read lines from STDIN (standard input)
for line in sys.stdin:
    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into rows using regex
    rows = re.split(regex, line)
    productid = rows[1]
    userid = rows[2]
    score = rows[6]
    # we skip the first line
    if score != "Score":
        print('%s\t%s\t%s' % (userid, productid, score))

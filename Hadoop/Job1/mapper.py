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

    timestamp = rows[7]
    text = rows[9]
    # we skip the first line
    if timestamp != "Time":
        date = datetime.fromtimestamp(int(timestamp)).strftime('%Y')
        print('%s\t\t%s' % (date, text))

#!/usr/bin/env python3
import sys
from datetime import datetime



# read input from stdin
for line in sys.stdin:

    try:
        # remove whitespaces and trailing characters
        line = line.strip()

        # parse name and unix date using TAB as a separator
        text, time = line.split("\t")

        # try to convert the unix date to an integer
        try:
            time = int(time)
        except ValueError:
            continue

        # build a datetime object from the unix time
        datetime_obj = datetime.utcfromtimestamp(time)

        # get the output date string
        year = datetime.fromtimestamp(int(time)).strftime('%Y')

        # print output items to stdout, using TAB as a separator
        print("\t".join([text, year]))
    except:
        import sys
        print(sys.exc_info())

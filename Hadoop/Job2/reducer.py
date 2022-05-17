#!/usr/bin/env python3
"""reducer.py"""

import sys

userID_2_products = {}

for line in sys.stdin:
    current_user, current_product, score = line.split("\t")

    # initialize userID_2_products dict and set if current user was not in there
    if current_user not in userID_2_products:
        userID_2_products[current_user] = []
    if current_user in userID_2_products:
        userID_2_products[current_user].append((current_product, score))

for user in userID_2_products:
    product_2_score_list = userID_2_products[user]
    ordered_list = sorted(product_2_score_list, key=lambda item: item[1][1], reverse=True)
    print("%s\t%s" % (user, ordered_list[:5]))

#!/usr/bin/env python3
"""reducer.py"""
import itertools

import sys

# this dictionary maps each user id with a set of product he scored at least 4
userID_2_products = {}
products_2_userID = {}
filtered_userID_2_products = {}

users_2_shared_products = {}
# input comes from STDIN
# note: this is the output from the mapper!


for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    current_user, current_product = line.split("\t\t")

    # initialize user2product dict and set if current user was not in there
    if current_user not in userID_2_products:
        userID_2_products[current_user] = set()
    if current_user in userID_2_products:
        userID_2_products[current_user].add(current_product)

    # initialize user2product dict and set if current user was not in there
    if current_product not in products_2_userID:
        products_2_userID[current_product] = set()
    if current_product in products_2_userID:
        products_2_userID[current_product].add(current_user)

# filter my dictionary with users that have reviewed at least 3 products
for u in userID_2_products:
    if len(userID_2_products[u]) >= 3:
        filtered_userID_2_products[u] = userID_2_products[u]
listUsers = filtered_userID_2_products.keys()

for product in products_2_userID.keys():
    pairs = list(itertools.combinations(products_2_userID[product], 2))

    for pair in pairs:
        if pair and tuple(reversed(pair)) not in users_2_shared_products:
            products_intersection = userID_2_products[pair[0]]. \
                intersection(userID_2_products[pair[1]])
            if len(products_intersection) >= 3:
                users_2_shared_products[pair] = products_intersection

OrderedKeys = sorted(users_2_shared_products.keys(), key=lambda x: (x[0], x[1]))
for pair in OrderedKeys:
    sharedProducts = users_2_shared_products[pair]
    print("%s\t%s" % (pair, sharedProducts))

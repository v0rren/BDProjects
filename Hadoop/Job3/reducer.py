#!/usr/bin/env python3
"""reducer.py"""
import itertools

import sys

# this dictionary maps each user id with a set of product he scored at least 4
userID_2_products = {}
filtered_userID_2_products = {}

users_2_shared_products = {}
# input comes from STDIN
# note: this is the output from the mapper!


for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    current_user, current_product = line.split("\t\t")

    # initialize dic and set if current user was not in there
    if current_user not in userID_2_products:
        userID_2_products[current_user] = set()
    if current_user in userID_2_products:
        userID_2_products[current_user].add(current_product)

# filter my dictionary with users that have reviewed at least 3 products
for u in userID_2_products:
    if len(userID_2_products[u]) >= 3:
        filtered_userID_2_products[u] = userID_2_products[u]
listUsers = filtered_userID_2_products.keys()

# create a generator of tuple removing the reversed tuple ( if I have (1,2) i don't want (2,1) and same value (1,1)
# also removing the tuples that have less than 3 products after intersecting their product list
list_of_tuples = (tuple(i) for i in itertools.product(tuple(listUsers), repeat=2) if tuple(reversed(i)) >= tuple(i) and i[0] != i[1] and len(filtered_userID_2_products[i[0]].intersection(filtered_userID_2_products[i[1]])) >= 3)

# somma = sum( 1 for tuples in list_of_tuples)
# print("%s\t%d" % ("somma filatrata", somma))


for tuples in list_of_tuples:

    if tuples[0] == tuples[1]:
        continue

    cur_bigram = tuples
    inverted_cur_bigram = reversed(cur_bigram)
    if cur_bigram and inverted_cur_bigram not in users_2_shared_products:
        users_2_shared_products[cur_bigram] = set()
    if inverted_cur_bigram in users_2_shared_products:
        cur_bigram = inverted_cur_bigram

    set1 = filtered_userID_2_products[cur_bigram[0]]
    set2 = filtered_userID_2_products[cur_bigram[1]]
    set3 = set1.intersection(set2)
    if len(set3) >= 3:
        print("%s\t%s" % (cur_bigram, set3))


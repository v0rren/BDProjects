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

# Create a list of tuples
# list_of_tuples = set(itertools.product(userID_2_products.keys(), userID_2_products.keys()))

for u in userID_2_products:
    if len(userID_2_products[u]) == 3:
        filtered_userID_2_products[u] = userID_2_products[u]
listUsers = filtered_userID_2_products.keys()
list_of_tuples = (tuple(i) for i in itertools.product(tuple(listUsers), repeat=2) if tuple(reversed(i)) >= tuple(i) and i[0] != i[1] )

for tuples in list(list_of_tuples):

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
        users_2_shared_products[cur_bigram].update(set1.intersection(set2))

for users in users_2_shared_products:
    shared_prod = list(users_2_shared_products[users])
    if len(shared_prod) != 0:
        print("%s\t%s" % (users, shared_prod[:3]))

#!/usr/bin/env python

import sys

curr_word = None

for line in sys.stdin:

      word, count = line.split('\t')

      count = int(count)

      if word != curr_word:
            curr_word = word
            print(curr_word)
    #   else:
    #       print("DUPLICATED:"+curr_word)

if curr_word == word:
      print(curr_word)
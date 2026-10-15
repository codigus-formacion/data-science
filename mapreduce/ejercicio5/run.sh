#!/bin/bash
cat dna.json | python mapper.py | sort -k1,1 | python reducer.py

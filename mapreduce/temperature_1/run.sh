#!/bin/bash
cat temperature_sample.txt | python mapper.py | sort -k1,1 | python reducer.py

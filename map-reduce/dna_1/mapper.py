#!/usr/bin/env python

import sys
import json

for line in sys.stdin:
    record = json.loads(line)
    print("{0}\t{1}".format(record[1][:10], 1))
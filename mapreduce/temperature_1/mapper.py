#!/usr/bin/env python

import re
import sys

for line in sys.stdin:
    val = line.strip()
    (year, temp) = (val[15:19], val[87:92])
    if (temp != "+9999"):
        print("%s\t%s" % (year, temp))
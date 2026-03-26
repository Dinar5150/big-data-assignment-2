#!/usr/bin/env python3
import sys


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    parts = line.split("\t")
    if len(parts) == 4 and parts[0] == "DOC":
        print(f"CORPUS\t{parts[3]}")

#!/usr/bin/env python3
import sys


doc_count = 0
total_length = 0

for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    parts = line.split("\t")
    if len(parts) != 2 or parts[0] != "CORPUS":
        continue

    doc_count += 1
    total_length += int(parts[1])

average = 0.0
if doc_count > 0:
    average = total_length / float(doc_count)

print(f"CORPUS\t{doc_count}\t{average}")

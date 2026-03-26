#!/usr/bin/env python3
import re
import sys
from collections import Counter


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    tokens = tokenize(text)
    if not tokens:
        continue

    doc_length = len(tokens)
    term_counts = Counter(tokens)

    print(f"DOC\t{doc_id}\t{title}\t{doc_length}")
    for term, tf in sorted(term_counts.items()):
        print(f"TERM\t{term}\t{doc_id}\t{title}\t{tf}\t{doc_length}")

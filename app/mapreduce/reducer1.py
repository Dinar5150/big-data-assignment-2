#!/usr/bin/env python3
import sys


current_term = None
term_rows = []


def flush_term(term, rows):
    if term is None:
        return

    df = len(rows)
    print(f"VOCAB\t{term}\t{df}")
    for doc_id, title, tf, doc_length in rows:
        print(f"INDEX\t{term}\t{df}\t{doc_id}\t{title}\t{tf}\t{doc_length}")


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    parts = line.split("\t")
    if not parts:
        continue

    tag = parts[0]

    if tag == "DOC" and len(parts) == 4:
        _, doc_id, title, doc_length = parts
        print(f"DOC\t{doc_id}\t{title}\t{doc_length}")
        continue

    if tag != "TERM" or len(parts) != 6:
        continue

    _, term, doc_id, title, tf, doc_length = parts

    if current_term is None:
        current_term = term

    if term != current_term:
        flush_term(current_term, term_rows)
        current_term = term
        term_rows = []

    term_rows.append((doc_id, title, tf, doc_length))


flush_term(current_term, term_rows)

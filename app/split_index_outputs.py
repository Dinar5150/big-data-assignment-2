import os
import sys


if len(sys.argv) != 4:
    raise SystemExit("Usage: python split_index_outputs.py pipeline1.tsv pipeline2.tsv output_dir")


pipeline1_path = sys.argv[1]
pipeline2_path = sys.argv[2]
output_dir = sys.argv[3]

os.makedirs(output_dir, exist_ok=True)

vocabulary = []
index = []
documents = []
stats = []

with open(pipeline1_path, "r", encoding="utf-8") as f:
    for raw_line in f:
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split("\t")

        if parts[0] == "VOCAB":
            vocabulary.append("\t".join(parts[1:3]))
        elif parts[0] == "INDEX":
            index.append("\t".join(parts[1:7]))
        elif parts[0] == "DOC":
            documents.append("\t".join(parts[1:4]))

with open(pipeline2_path, "r", encoding="utf-8") as f:
    for raw_line in f:
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if parts[0] == "CORPUS":
            stats.append(f"total_docs\t{parts[1]}")
            stats.append(f"avg_doc_length\t{parts[2]}")

with open(os.path.join(output_dir, "vocabulary.tsv"), "w", encoding="utf-8") as f:
    for line in vocabulary:
        f.write(line + "\n")

with open(os.path.join(output_dir, "index.tsv"), "w", encoding="utf-8") as f:
    for line in index:
        f.write(line + "\n")

with open(os.path.join(output_dir, "documents.tsv"), "w", encoding="utf-8") as f:
    for line in documents:
        f.write(line + "\n")

with open(os.path.join(output_dir, "stats.tsv"), "w", encoding="utf-8") as f:
    for line in stats:
        f.write(line + "\n")

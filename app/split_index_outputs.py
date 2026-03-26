import os
import sys


def ensure_output_dir(path):
    os.makedirs(path, exist_ok=True)


def write_lines(path, lines):
    with open(path, "w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(line)
            handle.write("\n")


def main():
    if len(sys.argv) != 4:
        raise SystemExit("Usage: python split_index_outputs.py pipeline1.tsv pipeline2.tsv output_dir")

    pipeline1_path = sys.argv[1]
    pipeline2_path = sys.argv[2]
    output_dir = sys.argv[3]

    ensure_output_dir(output_dir)

    vocabulary_lines = []
    index_lines = []
    document_lines = []
    stats_lines = []

    with open(pipeline1_path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue

            parts = line.split("\t")
            tag = parts[0]

            if tag == "VOCAB":
                vocabulary_lines.append("\t".join(parts[1:3]))
            elif tag == "INDEX":
                index_lines.append("\t".join(parts[1:7]))
            elif tag == "DOC":
                document_lines.append("\t".join(parts[1:4]))

    with open(pipeline2_path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if parts[0] == "CORPUS":
                stats_lines.append(f"total_docs\t{parts[1]}")
                stats_lines.append(f"avg_doc_length\t{parts[2]}")

    write_lines(os.path.join(output_dir, "vocabulary.tsv"), vocabulary_lines)
    write_lines(os.path.join(output_dir, "index.tsv"), index_lines)
    write_lines(os.path.join(output_dir, "documents.tsv"), document_lines)
    write_lines(os.path.join(output_dir, "stats.tsv"), stats_lines)


if __name__ == "__main__":
    main()

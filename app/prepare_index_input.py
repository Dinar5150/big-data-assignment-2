import os
import re
import sys


def clean_text(text):
    return re.sub(r"\s+", " ", text.strip())


def line_from_txt_file(file_path):
    file_name = os.path.basename(file_path)
    if not file_name.endswith(".txt"):
        return None

    base_name = file_name[:-4]
    if "_" not in base_name:
        return None

    doc_id, title = base_name.split("_", 1)
    with open(file_path, "r", encoding="utf-8") as handle:
        text = clean_text(handle.read())

    if not doc_id or not title or not text:
        return None

    return f"{doc_id}\t{title}\t{text}"


def prepare_from_directory(input_path):
    lines = []
    for file_name in sorted(os.listdir(input_path)):
        file_path = os.path.join(input_path, file_name)
        if not os.path.isfile(file_path):
            continue

        line = line_from_txt_file(file_path)
        if line is not None:
            lines.append(line)

    return lines


def prepare_from_file(input_path):
    if input_path.endswith(".txt"):
        line = line_from_txt_file(input_path)
        if line is None:
            raise RuntimeError("Text file name must follow <doc_id>_<doc_title>.txt")
        return [line]

    with open(input_path, "r", encoding="utf-8") as handle:
        return [clean_text(line) for line in handle if clean_text(line)]


def main():
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python prepare_index_input.py input_path output_file")

    input_path = sys.argv[1]
    output_file = sys.argv[2]

    if os.path.isdir(input_path):
        lines = prepare_from_directory(input_path)
    else:
        lines = prepare_from_file(input_path)

    if not lines:
        raise RuntimeError("No input documents found.")

    with open(output_file, "w", encoding="utf-8") as handle:
        for line in lines:
            handle.write(line)
            handle.write("\n")


if __name__ == "__main__":
    main()

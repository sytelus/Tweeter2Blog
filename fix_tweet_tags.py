import os
import sys

def process_md_files(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".md"):
                file_path = os.path.join(root, file)
                modify_file(file_path)

def modify_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        modified = False

        for i in range(len(lines)):
            if lines[i].strip() == "is_tweet: true":
                lines[i] = "tags:\n  - tweets\nis_tweet: true\n"
                modified = True
                break  # Assuming only one occurrence per file

        if modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            print(f"Modified: {file_path}")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory>")
        sys.exit(1)

    directory = sys.argv[1]
    if not os.path.isdir(directory):
        print("Invalid directory path.")
        sys.exit(1)

    process_md_files(directory)

import os
import argparse
import re

def process_md_files(folder_path):
    """
    Recursively processes .md files in the given folder path.
    Changes draft status and adds is_retweet for files meeting specific criteria.
    """
    total_files = 0
    altered_files = 0

    # Walk through all files and directories in the given path
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.md'):
                total_files += 1
                file_path = os.path.join(root, file)

                try:
                    # Read the file content
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                    # Check if the file meets the criteria
                    has_x_com = "… <https://x.com/" in content
                    has_thread_false = "is_thread: False" in content
                    has_sytelus = "… <https://x.com/sytelus" in content

                    if has_x_com and has_thread_false and not has_sytelus:
                        # Replace "draft: false" with "draft: true" (any line match)
                        modified_content = re.sub(r'(?m)^[ \t]*draft:[ \t]*false[ \t]*$', 'draft: true', content)

                        # Replace "is_tweet: true" with "is_tweet: true\nis_retweet: true"
                        modified_content = re.sub(r'(?m)^[ \t]*is_tweet:[ \t]*true[ \t]*$', 'is_tweet: true\nis_retweet: true', modified_content)

                        # Check if the content was actually modified
                        if content != modified_content:
                            # Write the modified content back to the file
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(modified_content)

                            print(f"Altered: {file_path}")
                            altered_files += 1

                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")

    # Print summary
    print(f"\nSummary: {altered_files} files altered out of {total_files} files examined.")

def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Process markdown files to update draft status and add retweet flag.')
    parser.add_argument('folder_path', help='Relative or absolute path to the folder containing markdown files')

    # Parse arguments
    args = parser.parse_args()
    folder_path = args.folder_path

    # Convert to absolute path if relative
    folder_path = os.path.abspath(folder_path)

    # Check if the folder exists
    if not os.path.isdir(folder_path):
        print(f"Error: The specified folder path '{folder_path}' does not exist or is not a directory.")
        return

    print(f"Processing markdown files in: {folder_path}")

    # Process the files
    process_md_files(folder_path)

if __name__ == "__main__":
    main()
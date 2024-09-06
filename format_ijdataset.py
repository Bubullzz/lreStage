import os
import json
import sys


def get_file_text(file_path):
    """Reads the file's content."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        # Print the file path and the error message
        print(f"Error reading {file_path}: {e}\n")
        return []  # Or handle the error in another way, like returning an empty string.


def format_file_info(directory, filename):
    """Formats the file information as per the required structure."""
    file_path = os.path.join(directory, filename)
    file_text = get_file_text(file_path)
    file_length = len(file_text)
    return {"text": file_text, "url": f"{directory.split('/')[-1]},{filename},0,{file_length}"}


def process_directory(directory):
    """Processes all files in the given directory and returns the list of formatted data."""
    file_info_list = []
    for filename in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, filename)):
            file_info_list.append(format_file_info(directory, filename))
    return file_info_list


def main():
    directories = ["default", "sample", "selected"]  # List your directories here
    all_files_info = []
    ijset_dir = os.path.join(os.getcwd(), sys.argv[1])
    for num_dir in os.listdir(ijset_dir):
        for directory in directories:
            directory = os.path.join(ijset_dir, num_dir, directory)
            if os.path.exists(directory):
                all_files_info.extend(process_directory(directory))

    for f in all_files_info:
        print(json.dumps(f))

if __name__ == "__main__":
    main()

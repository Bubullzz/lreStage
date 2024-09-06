#!/bin/bash

# Check if input file is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <path_to_input> [--clean]"
    exit 1
fi

# Assign the formatted file passed as an argument
input="$1"
file1="all_duplicates"
file2="grouped_duplicates"
file3="final_formatted_duplicates"

# Step 1: Run find_duplicates.py
echo "Running find_duplicates.py..."
python3 find_duplicates.py --inputs "${input}" url --output "$file1"

# Step 2: Run group_duplicate_url.py
echo "Running group_duplicate_url.py..."
python3 group_duplicate_url.py "$file1" "$file2"
python3 format_big_code.py "$file2" "$file3"
# Optionally clean up temporary files
if [ "$2" == "--clean" ]; then
    echo "Cleaning up temporary files..."
    rm -f "$file1" "$file2" "$file3"
    echo "Temporary files removed."
fi

echo "Processing completed. duplicated pairs in: $file3"

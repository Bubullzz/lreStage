import os

# Specify the input file and number of output files
input_file = 'url_to_swhid'  # Replace with your input filename
num_files = 128

# Read all lines from the input file
print('reading')
with open(input_file, 'r') as f:
    lines = f.readlines()
print('done reading')

# Calculate how many lines each file should have
lines_per_file = len(lines) // num_files
remainder = len(lines) % num_files

# Create the output files and distribute the lines
for i in range(num_files):
    print(i)
    filename = f'urls/output_{i+1}.txt'
    with open(filename, 'w') as f:
        # Determine the start and end indices for the lines in this file
        start_index = i * lines_per_file + min(i, remainder)
        end_index = start_index + lines_per_file + (1 if i < remainder else 0)
        
        # Write the appropriate lines to the file
        f.writelines(lines[start_index:end_index])

print(f'Distributed {len(lines)} lines across {num_files} files. {lines_per_file} lines per file.')


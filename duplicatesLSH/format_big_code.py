import json
import sys

input = sys.argv[1]
output = sys.argv[2]

with open(input, 'r', encoding='utf-8') as ifile:
    with open(output, 'w', encoding='utf-8') as ofile:
        for line in ifile:
            parsed_line = json.loads(line.strip())
            similar_elt = list(parsed_line.values())[0]
            for i in range(len(similar_elt)):
                for j in range(i + 1, len(similar_elt)):
                    ofile.write(f"{similar_elt[i]},{similar_elt[j]}\n")


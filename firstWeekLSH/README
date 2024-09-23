go to BigCloneEval/bigclonebenchdb/ and dl from readme
go to BigCloneEval/ijadataset/ and dl from readme
make in BigCloneEval
go to LSH/ and run 'python setup.py install' (readme)
$ python3 format_ijdataset.py BigCloneEval/ijadataset/bcb_reduced/ > ijaset_formatted
$ cd duplicatesLS/
$ ./get_duplicates_pairs.sh ../../../ijaset_formatted
$ cd ../BigCloneEval/commands
$ ./registerTool -n name -d desc
this will print the id of the tool = x
$ ./importClones -c ../../duplicatesLSH/final_formatted_duplicates -t x
$ ./evaluateTool -t 1 -o results.txt
$ cat results.txt

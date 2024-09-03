from datasketch import MinHash, MinHashLSH
import re

treshold = 0.9
perms = 6  # MUST BE THE SAME FOR MinHash and MinHashLSH

m1 = MinHash(num_perm=perms)
m2 = MinHash(num_perm=perms)


def str_to_set(s):
    words_list = re.split(r'[ \t\n\r,:.]+', s)

    # Remove any empty strings from the list
    words_list = [word for word in words_list if word]

    # Convert list to set to get unique words
    return set(words_list)


set1 = str_to_set("""
if a == True :
    return 'Bonjour'
else:
    if b == 'Bonjour' :
        return a
""")


set2 = str_to_set("""
if b == 'Bonjour':
    return True
if a == True:
    if b == True :
        return b
    else:
        return True
""")

for item in set1:
    m1.update(item.encode('utf8'))

for item in set2:
    m2.update(item.encode('utf8'))

# We should see that set1 and set2 are the same even though the string are different
print(set1)
print(set2)

# Logically, the signatures are the same
print(m1.hashvalues)
print(m2.hashvalues)


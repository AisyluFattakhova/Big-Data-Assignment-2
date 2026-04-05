import sys
# Since the mapper already gave the total for the doc, 
# and each doc is only on one line in our input,
# the reducer just passes the data through
for line in sys.stdin:
    print(line.strip())
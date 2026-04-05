#!/usr/bin/env python3
import sys

current_word = None
current_doc_counts = {} # {doc_id: count}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Hadoop sends us: word [TAB] doc_id
    word, doc_id = line.split('\t', 1)

    # If this is the same word as before, just update the count for this document
    if current_word == word:
        # Still on the same word, count this document
        current_doc_counts[doc_id] = current_doc_counts.get(doc_id, 0) + 1
    else:
        # print the results for the OLD word
        if current_word:
            # Format: word [TAB] doc1:count,doc2:count
            results = "|".join([f"{d}:{c}" for d, c in current_doc_counts.items()])
            print(f"{current_word}\t{results}")
        
        # Reset for the new word
        current_word = word
        current_doc_counts = {doc_id: 1}

# Print the very last word
if current_word:
    results = "|".join([f"{d}:{c}" for d, c in current_doc_counts.items()])
    print(f"{current_word}\t{results}")
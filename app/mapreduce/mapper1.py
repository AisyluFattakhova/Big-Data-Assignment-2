#!/usr/bin/env python3
import sys
import re

# List of words to ignore
STOPWORDS = set(['a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 'to', 'was', 'were', 'will', 'with'])

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    
    parts = line.split('\t')
    if len(parts) < 3: continue
        
    doc_id = parts[0]
    # We can use the Title instead of ID if you prefer seeing titles in the index
    doc_title = parts[1] 
    text = parts[2]
    combined = f"{doc_id}_{doc_title}"
    # CHANGE HERE: Only keep actual letters (A-Z), remove all numbers and symbols
    text = re.sub(r'[^a-zA-Z\s]', '', text).lower()
    
    words = text.split()

    for word in words:
        if word not in STOPWORDS and len(word) > 2: # Ignore tiny words like 'id'
            # Let's print word [TAB] title so it makes more sense to you
            print(f"{word}\t{combined}")
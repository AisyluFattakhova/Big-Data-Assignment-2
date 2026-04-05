
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    parts = line.split('\t')
    if len(parts) < 3:
        continue
        
    doc_id = parts[0]
    text = parts[2]
    
    word_count = len(text.split())
    
    
    print(f"{doc_id}\t{word_count}")
import sqlite3
import json
from pathlib import Path

db = sqlite3.connect("queue.db")
db.row_factory = sqlite3.Row

# Get all completed items from the batch (id >= 8)
cursor = db.execute("""
    SELECT id, created_at, payload, metadata 
    FROM page_queue 
    WHERE id >= 8 AND status = 'completed'
    ORDER BY id
""")

items = cursor.fetchall()

if items:
    first_created = items[0]['created_at']
    last_created = items[-1]['created_at']
    
    # Calculate processing time from logs
    total_chunks = 0
    total_text = 0
    total_images = 0
    
    for item in items:
        metadata = json.loads(item['metadata'])
        num_chunks = metadata.get('num_chunks', 0)
        total_chunks += num_chunks
        
        # Read the output file if it exists
        output_file = metadata.get('output_file')
        if output_file and Path(output_file).exists():
            with open(output_file) as f:
                data = json.load(f)
                for chunk in data['chunks']:
                    if chunk['type'] == 'text':
                        total_text += 1
                    elif chunk['type'] == 'image':
                        total_images += 1
    
    # Time calculation
    time_span = last_created - first_created
    
    print(f"{'='*70}")
    print(f"WIKIPEDIA PROCESSING PERFORMANCE REPORT")
    print(f"{'='*70}")
    print(f"\nBatch Details:")
    print(f"  URLs submitted:     99")
    print(f"  Successfully processed: {len(items)}")
    print(f"  Failed:            {99 - len(items)}")
    print(f"  Success rate:      {(len(items)/99)*100:.1f}%")
    
    print(f"\nProcessing Time:")
    print(f"  Time span:         {time_span:.2f} seconds ({time_span/60:.2f} minutes)")
    print(f"  Avg per page:      {time_span/len(items):.2f} seconds")
    print(f"  Throughput:        {len(items)/(time_span/60):.1f} pages/minute")
    
    print(f"\nContent Extracted:")
    print(f"  Total chunks:      {total_chunks:,}")
    print(f"  Text chunks:       {total_text:,}")
    print(f"  Image chunks:      {total_images:,}")
    print(f"  Avg chunks/page:   {total_chunks/len(items):.1f}")
    print(f"  Avg images/page:   {total_images/len(items):.1f}")
    
    print(f"\nTop 10 Pages by Content:")
    # Get top pages
    page_stats = []
    for item in items:
        metadata = json.loads(item['metadata'])
        payload = json.loads(item['payload'])
        url = payload['page_url'].split('/')[-1]
        chunks = metadata.get('num_chunks', 0)
        page_stats.append((url, chunks))
    
    page_stats.sort(key=lambda x: x[1], reverse=True)
    for i, (page, chunks) in enumerate(page_stats[:10], 1):
        print(f"  {i:2}. {page:40} {chunks:4} chunks")
    
    print(f"\n{'='*70}")

db.close()

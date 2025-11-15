"""Quick performance analysis of threaded worker."""

import sqlite3
import json

db = sqlite3.connect("queue.db")
db.row_factory = sqlite3.Row

# Get items processed by the threaded worker (items >= 158)
cursor = db.execute("""
    SELECT id, created_at, payload, metadata, status
    FROM page_queue
    WHERE id >= 158 AND id <= 207
    ORDER BY id
""")

items = cursor.fetchall()

completed = [item for item in items if item['status'] == 'completed']
failed = [item for item in items if item['status'] == 'failed']

if completed:
    first_time = completed[0]['created_at']
    last_metadata = json.loads(completed[-1]['metadata'])
    last_time = last_metadata.get('completed_at', completed[-1]['created_at'])

    time_span = last_time - first_time

    # Count chunks
    total_chunks = sum(
        json.loads(item['metadata']).get('num_chunks', 0)
        for item in completed
    )

    print(f"{'='*70}")
    print(f"THREADED WORKER PERFORMANCE (4 threads)")
    print(f"{'='*70}")
    print(f"\nBatch Details:")
    print(f"  URLs submitted:        50")
    print(f"  Successfully processed: {len(completed)}")
    print(f"  Failed:                {len(failed)}")
    print(f"  Success rate:          {(len(completed)/50)*100:.1f}%")

    print(f"\nProcessing Time:")
    print(f"  Time span:            {time_span:.2f} seconds ({time_span/60:.2f} minutes)")
    print(f"  Avg per page:         {time_span/len(completed):.2f} seconds")
    print(f"  Throughput:           {len(completed)/(time_span/60):.1f} pages/minute")

    print(f"\nContent Extracted:")
    print(f"  Total chunks:         {total_chunks:,}")
    print(f"  Avg chunks/page:      {total_chunks/len(completed):.1f}")

    print(f"\n{'='*70}")
    print(f"COMPARISON")
    print(f"{'='*70}")
    print(f"  Single-threaded:      46 pages/minute")
    print(f"  Threaded (4 threads): {len(completed)/(time_span/60):.1f} pages/minute")
    print(f"  Beam (4 workers):     161.3 pages/minute")
    print(f"\n  Threaded speedup:     {(len(completed)/(time_span/60))/46:.2f}x over single-threaded")
    print(f"  Beam speedup:         {161.3/46:.2f}x over single-threaded")
    print(f"  Beam vs Threaded:     {161.3/(len(completed)/(time_span/60)):.2f}x faster")
    print(f"{'='*70}")

db.close()

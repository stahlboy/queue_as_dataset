#!/bin/bash
echo "Time,Total,Pending,Processing,Completed,Failed,Rate(items/min)"
start_time=$(date +%s)
prev_completed=0

for i in {1..30}; do
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    stats=$(curl -s http://localhost:8000/api/stats)
    total=$(echo $stats | jq -r '.total')
    pending=$(echo $stats | jq -r '.pending')
    processing=$(echo $stats | jq -r '.processing')
    completed=$(echo $stats | jq -r '.completed')
    failed=$(echo $stats | jq -r '.failed')
    
    # Calculate rate
    if [ $elapsed -gt 0 ]; then
        items_processed=$((completed - prev_completed))
        rate=$(echo "scale=2; ($items_processed * 60) / 10" | bc)
    else
        rate=0
    fi
    
    timestamp=$(date +%H:%M:%S)
    echo "$timestamp,$total,$pending,$processing,$completed,$failed,$rate"
    
    prev_completed=$completed
    
    # Stop if all done
    if [ $pending -eq 0 ] && [ $processing -eq 0 ]; then
        echo "Processing complete!"
        break
    fi
    
    sleep 10
done

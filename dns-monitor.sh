#!/bin/bash

# DNS Workers Live Dashboard
# Real-time monitoring of DNS processing

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Function to get queue stats
get_queue_stats() {
    if curl -s -u guest:guest http://localhost:15672/api/queues/%2F/dns_records >/dev/null 2>&1; then
        curl -s -u guest:guest http://localhost:15672/api/queues/%2F/dns_records | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f\"{data.get('messages', 0)}|{data.get('consumers', 0)}|{data.get('message_stats', {}).get('publish_details', {}).get('rate', 0):.1f}|{data.get('message_stats', {}).get('deliver_get_details', {}).get('rate', 0):.1f}\")
except:
    print('0|0|0.0|0.0')
"
    else
        echo "0|0|0.0|0.0"
    fi
}

# Function to get worker stats
get_worker_stats() {
    docker-compose ps -q worker 2>/dev/null | wc -l
}

# Main monitoring loop
monitor_live() {
    echo "🔍 DNS Workers Live Dashboard"
    echo "Press Ctrl+C to exit"
    echo ""
    
    while true; do
        clear
        echo -e "${BLUE}═══════════════════════════════════════════════════${NC}"
        echo -e "${BLUE}           DNS Processing Live Dashboard           ${NC}"
        echo -e "${BLUE}═══════════════════════════════════════════════════${NC}"
        echo ""
        
        # Time
        echo -e "${YELLOW}📅 $(date)${NC}"
        echo ""
        
        # Worker Status
        worker_count=$(get_worker_stats)
        if [ "$worker_count" -gt 0 ]; then
            echo -e "${GREEN}👥 Workers: $worker_count active${NC}"
        else
            echo -e "${RED}👥 Workers: No workers running${NC}"
        fi
        
        # Queue Stats
        stats=$(get_queue_stats)
        IFS='|' read -r messages consumers publish_rate consume_rate <<< "$stats"
        
        echo -e "${BLUE}📊 Queue Status:${NC}"
        echo "   📨 Messages waiting: $messages"
        echo "   🔌 Active consumers: $consumers"
        echo "   📈 Publish rate: $publish_rate/sec"
        echo "   📉 Consume rate: $consume_rate/sec"
        echo ""
        
        # Container Status
        echo -e "${BLUE}🐳 Container Status:${NC}"
        docker-compose ps --format "   {{.Name}}: {{.Status}}" 2>/dev/null || echo "   No containers running"
        echo ""
        
        # Resource Usage (if workers running)
        if [ "$worker_count" -gt 0 ]; then
            echo -e "${BLUE}💻 Resource Usage:${NC}"
            docker stats --no-stream --format "   {{.Name}}: CPU {{.CPUPerc}}, Memory {{.MemUsage}}" $(docker-compose ps -q) 2>/dev/null || echo "   Unable to get stats"
            echo ""
        fi
        
        # Processing Rate Estimate
        if [ "$consume_rate" != "0.0" ] && [ "$messages" -gt 0 ]; then
            eta_seconds=$(echo "$messages / $consume_rate" | bc -l 2>/dev/null || echo "0")
            eta_minutes=$(echo "$eta_seconds / 60" | bc -l 2>/dev/null || echo "0")
            echo -e "${YELLOW}⏱️  ETA to complete queue: ${eta_minutes%.*} minutes${NC}"
            echo ""
        fi
        
        echo -e "${BLUE}═══════════════════════════════════════════════════${NC}"
        echo "🔄 Refreshing in 5 seconds... (Ctrl+C to exit)"
        
        sleep 5
    done
}

# Run the monitor
monitor_live
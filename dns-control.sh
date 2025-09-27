#!/bin/bash

# DNS Workers Management Script
# Complete control interface for DNS processing services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Function to check if services are running
check_host_services() {
    echo "🔍 Checking host services..."
    
    # Check PostgreSQL
    if pg_isready -h localhost -p 5432 >/dev/null 2>&1; then
        print_status "PostgreSQL is running"
    else
        print_error "PostgreSQL is not running or not accessible"
    fi
    
    # Check RabbitMQ
    if curl -s -u guest:guest http://localhost:15672/api/overview >/dev/null 2>&1; then
        print_status "RabbitMQ is running"
    else
        print_error "RabbitMQ is not running or not accessible"
    fi
}

# Function to show worker status
show_status() {
    echo "📊 DNS Services Status:"
    echo "======================"
    
    docker-compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
    
    echo ""
    echo "🔢 Worker Count:"
    worker_count=$(docker-compose ps -q worker 2>/dev/null | wc -l)
    echo "  Currently running: $worker_count workers"
    
    if [ "$worker_count" -gt 0 ]; then
        echo ""
        echo "📈 Resource Usage:"
        docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" $(docker-compose ps -q)
    fi
}

# Function to start services
start_services() {
    local workers=${1:-4}
    
    echo "🚀 Starting DNS services with $workers workers..."
    check_host_services
    
    # Start publisher first
    docker-compose up -d publisher
    
    # Start workers
    docker-compose up --scale worker=$workers -d worker
    
    echo "⏳ Waiting for services to initialize..."
    sleep 3
    
    show_status
}

# Function to stop services
stop_services() {
    echo "🛑 Stopping DNS services..."
    docker-compose down
    print_status "All services stopped"
}

# Function to restart services
restart_services() {
    local workers=${1:-4}
    
    echo "🔄 Restarting DNS services..."
    stop_services
    sleep 2
    start_services $workers
}

# Function to scale workers
scale_workers() {
    local workers=$1
    
    if [ -z "$workers" ]; then
        print_error "Please specify number of workers"
        echo "Usage: $0 scale <number>"
        exit 1
    fi
    
    echo "📈 Scaling to $workers workers..."
    docker-compose up --scale worker=$workers -d worker
    
    echo "⏳ Waiting for scaling to complete..."
    sleep 3
    show_status
}

# Function to show logs
show_logs() {
    local service=${1:-""}
    local follow=${2:-""}
    
    if [ -z "$service" ]; then
        echo "📋 Available services for logs:"
        echo "  - publisher (domain publisher)"
        echo "  - worker (DNS workers)"
        echo "  - all (all services)"
        echo ""
        echo "Usage: $0 logs <service> [follow]"
        echo "Example: $0 logs worker follow"
        return
    fi
    
    if [ "$service" = "all" ]; then
        if [ "$follow" = "follow" ]; then
            docker-compose logs -f
        else
            docker-compose logs --tail=100
        fi
    else
        if [ "$follow" = "follow" ]; then
            docker-compose logs -f $service
        else
            docker-compose logs --tail=100 $service
        fi
    fi
}

# Function to monitor queue
monitor_queue() {
    echo "📊 RabbitMQ Queue Monitoring:"
    echo "============================="
    
    # Check if RabbitMQ management is accessible
    if ! curl -s -u guest:guest http://localhost:15672/api/overview >/dev/null; then
        print_error "Cannot access RabbitMQ Management API"
        return 1
    fi
    
    # Get queue info
    queue_info=$(curl -s -u guest:guest http://localhost:15672/api/queues/%2F/dns_records 2>/dev/null)
    
    if [ "$?" -eq 0 ] && [ "$queue_info" != "Object Not Found" ]; then
        messages=$(echo $queue_info | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('messages', 'N/A'))")
        consumers=$(echo $queue_info | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('consumers', 'N/A'))")
        
        echo "  Queue: dns_records"
        echo "  Messages: $messages"
        echo "  Consumers: $consumers"
        echo "  Management UI: http://localhost:15672"
    else
        print_warning "Queue 'dns_records' not found or empty"
    fi
}

# Function to clean up
cleanup() {
    echo "🧹 Cleaning up DNS services..."
    
    # Stop and remove containers
    docker-compose down --remove-orphans
    
    # Remove any orphaned containers
    docker rm -f $(docker ps -aq --filter "name=dns-") 2>/dev/null || true
    
    print_status "Cleanup completed"
}

# Function to show help
show_help() {
    echo "DNS Workers Control Script"
    echo "========================="
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start [workers]     - Start services with N workers (default: 4)"
    echo "  stop               - Stop all services"
    echo "  restart [workers]  - Restart services with N workers"
    echo "  scale <number>     - Scale workers to specific number"
    echo "  status             - Show service status and resource usage"
    echo "  logs <service>     - Show logs (services: publisher, worker, all)"
    echo "  monitor            - Monitor RabbitMQ queue status"
    echo "  cleanup            - Stop and remove all containers"
    echo "  check              - Check host services (PostgreSQL, RabbitMQ)"
    echo ""
    echo "Examples:"
    echo "  $0 start 10        - Start with 10 workers"
    echo "  $0 scale 20        - Scale to 20 workers"
    echo "  $0 logs worker follow - Follow worker logs"
    echo "  $0 monitor         - Show queue status"
}

# Main script logic
case "${1:-help}" in
    "start")
        start_services ${2:-4}
        ;;
    "stop")
        stop_services
        ;;
    "restart")
        restart_services ${2:-4}
        ;;
    "scale")
        scale_workers $2
        ;;
    "status")
        show_status
        ;;
    "logs")
        show_logs $2 $3
        ;;
    "monitor")
        monitor_queue
        ;;
    "cleanup")
        cleanup
        ;;
    "check")
        check_host_services
        ;;
    "help"|"--help"|"-h")
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
#!/bin/bash
# Post-installation setup script
# Configures and starts the pipeline services

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Error handling
error_exit() {
    echo -e "${RED}Error: $1${NC}" >&2
    exit 1
}

# Check prerequisites
check_prerequisites() {
    echo -e "${GREEN}Checking prerequisites...${NC}"
    
    if ! command -v docker &> /dev/null; then
        error_exit "Docker is not installed"
    fi
    
    if ! command -v docker compose &> /dev/null && ! command -v docker-compose &> /dev/null; then
        error_exit "Docker Compose is not installed"
    fi
    
    # Validate environment
    if [ -f "validate_env.sh" ]; then
        bash validate_env.sh || error_exit "Environment validation failed"
    fi
}

# Configure broker logging
configure_logging() {
    echo -e "${GREEN}Configuring broker logging...${NC}"
    if [ -f "configure_broker_logging.sh" ]; then
        bash configure_broker_logging.sh || echo -e "${YELLOW}Warning: Logging configuration failed${NC}"
    else
        echo -e "${YELLOW}Warning: configure_broker_logging.sh not found${NC}"
    fi
}

# Build and start services
start_services() {
    echo -e "${GREEN}Building and starting services...${NC}"
    docker compose up -d --build || error_exit "Failed to start services"
    
    echo -e "${GREEN}Waiting for services to be healthy...${NC}"
    sleep 10
}

# Run database migrations
run_migrations() {
    echo -e "${GREEN}Running Airflow database migrations...${NC}"
    docker compose run --rm apiserver airflow db migrate || error_exit "Database migration failed"
}

# Create Kafka topics
create_kafka_topics() {
    echo -e "${GREEN}Creating Kafka topics...${NC}"
    docker compose exec -T broker bash -lc \
        'kafka-topics --bootstrap-server broker:29092 --create --if-not-exists --topic _confluent-telemetry-metrics --partitions 12 --replication-factor 1' \
        || echo -e "${YELLOW}Warning: Failed to create Kafka topic${NC}"
}

# Display status
show_status() {
    echo -e "${GREEN}Service status:${NC}"
    docker compose ps
    
    echo ""
    echo -e "${GREEN}Setup completed successfully!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Create Airflow admin user:"
    echo "   docker compose run --rm apiserver airflow users create \\"
    echo "     --username admin --firstname Admin --lastname User \\"
    echo "     --role Admin --email admin@example.com --password 'CHANGE_ME'"
    echo ""
    echo "2. Access services:"
    echo "   - Airflow: http://localhost:8080"
    echo "   - Control Center: http://localhost:9021"
    echo "   - Spark UI: http://localhost:9090"
}

# Main execution
main() {
    echo -e "${GREEN}Starting post-installation setup...${NC}"
    echo ""
    
    check_prerequisites
    configure_logging
    start_services
    run_migrations
    create_kafka_topics
    show_status
}

# Run main function
main

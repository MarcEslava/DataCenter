#!/bin/bash
# Script to configure broker logging to reduce log verbosity
# This script updates log4j.properties to use ERROR level instead of WARN/INFO

LOG4J_FILE="secrets/kafka/log4j.properties"

if [ ! -f "$LOG4J_FILE" ]; then
    echo "Error: $LOG4J_FILE not found"
    exit 1
fi

# Create backup
cp "$LOG4J_FILE" "${LOG4J_FILE}.backup"

# Update log4j.properties with reduced logging configuration
cat > "$LOG4J_FILE" << 'EOF'

log4j.rootLogger=ERROR, stdout

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=[%d] %p %m (%c)%n

# Reduce verbosity for specific noisy components - set to ERROR to minimize logs
log4j.logger.kafka=ERROR
log4j.logger.org.apache.kafka=ERROR
log4j.logger.org.apache.zookeeper=ERROR
log4j.logger.org.I0Itec.zkclient=ERROR
log4j.logger.kafka.controller=ERROR
log4j.logger.kafka.log.LogCleaner=ERROR
log4j.logger.state.change.logger=ERROR
log4j.logger.kafka.network.RequestChannel$=ERROR
log4j.logger.kafka.request.logger=ERROR
log4j.logger.kafka.log=ERROR
log4j.logger.kafka.server=ERROR
log4j.logger.kafka.common=ERROR
# Confluent-specific loggers
log4j.logger.io.confluent=ERROR
log4j.logger.com.confluent=ERROR
EOF

echo "Successfully updated $LOG4J_FILE to use ERROR logging level"
echo "Backup saved to ${LOG4J_FILE}.backup"

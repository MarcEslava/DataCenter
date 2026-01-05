#!/bin/bash
# Environment validation script
# Checks that all required environment variables are set

set -e

ERRORS=0
WARNINGS=0

echo "🔍 Validating environment configuration..."

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found"
    echo "   Please copy env_example.txt to .env and configure it"
    ERRORS=$((ERRORS + 1))
else
    echo "✅ .env file found"
    # Source .env file
    set -a
    source .env
    set +a
fi

# Required environment variables
REQUIRED_VARS=(
    "POSTGRES_USER"
    "POSTGRES_PASSWORD"
    "POSTGRES_DB"
    "AIRFLOW_DB_URL"
)

# Check required variables
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Error: $var is not set"
        ERRORS=$((ERRORS + 1))
    else
        echo "✅ $var is set"
    fi
done

# Check secrets directory
if [ ! -d "secrets/kafka" ]; then
    echo "⚠️  Warning: secrets/kafka directory not found"
    WARNINGS=$((WARNINGS + 1))
else
    echo "✅ secrets/kafka directory exists"
    
    # Check critical secret files
    CRITICAL_SECRETS=(
        "secrets/kafka/jaas.conf"
        "secrets/kafka/kafka.properties"
        "secrets/kafka/log4j.properties"
    )
    
    for secret in "${CRITICAL_SECRETS[@]}"; do
        if [ ! -f "$secret" ]; then
            echo "⚠️  Warning: $secret not found"
            WARNINGS=$((WARNINGS + 1))
        else
            echo "✅ $(basename $secret) exists"
        fi
    done
fi

# Check Docker and Docker Compose
if ! command -v docker &> /dev/null; then
    echo "❌ Error: docker command not found"
    ERRORS=$((ERRORS + 1))
else
    echo "✅ Docker is installed"
fi

if ! command -v docker compose &> /dev/null && ! command -v docker-compose &> /dev/null; then
    echo "❌ Error: docker compose command not found"
    ERRORS=$((ERRORS + 1))
else
    echo "✅ Docker Compose is installed"
fi

# Summary
echo ""
if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo "✅ All checks passed!"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo "⚠️  Validation completed with $WARNINGS warning(s)"
    exit 0
else
    echo "❌ Validation failed with $ERRORS error(s) and $WARNINGS warning(s)"
    exit 1
fi

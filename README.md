# Pipeline

A cross-platform, extensible data pipeline for building, testing and deploying projects. Integrates Apache Airflow, Confluent Kafka, Apache Spark, and PostgreSQL in a containerized environment.

## 🏗️ Architecture

This pipeline includes:
- **Apache Airflow 3.1.3** - Workflow orchestration (API Server, Scheduler, DAG Processor)
- **Confluent Platform 7.4.0** - Kafka ecosystem (Broker, Schema Registry, Control Center, Connect)
- **Apache Spark 4.0.1** - Distributed data processing (Master/Worker)
- **PostgreSQL 14** - Database backend
- **Python 3.11** - Custom container with PySpark, Kafka, and MongoDB clients

## 📋 Prerequisites

- Docker and Docker Compose
- Git
- Shell (bash or PowerShell)

## 🚀 Quick Start

### 1. Clone the repository
```bash
git clone <repo-url>
cd pipeline
```

### 2. Set up environment variables
```bash
# Copy the example environment file
cp env_example.txt .env

# Edit .env with your values
# Required variables:
# - POSTGRES_USER
# - POSTGRES_PASSWORD
# - POSTGRES_DB
# - AIRFLOW_DB_URL
```

### 3. Configure secrets
Place your secrets in `./secrets/kafka/`:
- `jaas.conf` - Kafka authentication configuration
- `kafka.properties` - Kafka broker properties
- `log4j.properties` - Broker logging configuration (auto-configured by script)
- `zookeeper_jaas.conf` - ZooKeeper authentication
- `zookeeper.properties` - ZooKeeper properties
- `server.keystore.jks` - SSL keystore (see TLS section)
- `server.truststore.jks` - SSL truststore
- Credential files: `keystore_creds`, `key_creds`, `truststore_creds`

### 4. Run the setup script
```bash
# Linux/Mac
bash post_install.sh

# Windows PowerShell
.\post_install.sh
```

The script will:
- Configure broker logging (reduces log verbosity)
- Build and start all services
- Run Airflow database migrations
- Display service status

### 5. Create Airflow admin user
```bash
docker compose run --rm apiserver airflow users create \
    --username admin \
    --firstname Admin --lastname User \
    --role Admin \
    --email admin@yourco.com \
    --password 'CHANGE_ME'
```

## 🔧 Services

### Service Ports
| Service | Port | Description |
|---------|------|-------------|
| Airflow API Server | 8080 | Airflow web UI and API |
| Kafka Broker | 9092 | External Kafka listener |
| Kafka Broker (Internal) | 29092 | Internal Kafka listener |
| Kafka JMX | 9101 | JMX monitoring port |
| Control Center | 9021 | Confluent Control Center UI |
| Schema Registry | 8081 | Schema Registry API |
| Kafka Connect | 8083 | Kafka Connect REST API |
| Spark Master UI | 9090 | Spark web UI |
| Spark Master | 7077 | Spark master port |
| Reverse Proxy (dev) | 8088 | Apache HTTPD (dev profile) |

### Service URLs
- **Airflow**: http://localhost:8080
- **Control Center**: http://localhost:9021
- **Spark UI**: http://localhost:9090
- **Schema Registry**: http://localhost:8081

## 📁 Project Structure

```
pipeline/
├── apps/                    # Application code
│   └── spark_stream.py      # Spark streaming application
├── conf/                     # Configuration files
│   ├── airflow.cfg          # Airflow configuration
│   └── httpd_conf/          # Apache HTTPD configuration
├── dags/                     # Airflow DAGs
├── secrets/                   # Secrets (not in git)
│   └── kafka/               # Kafka secrets and configs
├── secrets_example/          # Example secret templates
├── data/                     # Data directory
├── docker-compose.yaml      # Main compose file
├── Dockerfile               # Python master container
├── Dockerfile.airflow       # Custom Airflow image
├── requirements.txt         # Python dependencies
├── post_install.sh         # Setup script
└── configure_broker_logging.sh  # Logging configuration script
```

## 🔐 Security & TLS

### Generate Kafka SSL Certificates

1. **Generate keystore and keypair**
```bash
docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:17-jdk-jammy \
    keytool -genkeypair -alias kafka-broker \
    -keystore server.keystore.jks -storepass CHANGE_ME -keypass CHANGE_ME \
    -keyalg RSA -keysize 4096 -validity 3650 \
    -dname "CN=kafka.example.com, OU=Dev, O=Org, L=City, ST=Region, C=US" \
    -ext "SAN=dns:kafka.example.com"
```

2. **Export public certificate (PEM)**
```bash
docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:17-jdk-jammy \
    keytool -exportcert -alias kafka-broker \
    -keystore server.keystore.jks -storepass CHANGE_ME \
    -rfc -file server.crt
```

3. **Create truststore and import cert**
```bash
docker run --rm -v "${PWD}:/work" -w /work eclipse-temurin:17-jdk-jammy \
    keytool -importcert -alias kafka-broker \
    -file server.crt -keystore server.truststore.jks \
    -storepass CHANGE_ME -noprompt
```

4. **Create password files**

**PowerShell:**
```powershell
Set-Content -NoNewline -Path secrets/kafka/keystore_creds -Value 'CHANGE_ME'
Set-Content -NoNewline -Path secrets/kafka/key_creds -Value 'CHANGE_ME'
Set-Content -NoNewline -Path secrets/kafka/truststore_creds -Value 'CHANGE_ME'
```

**Bash:**
```bash
printf 'CHANGE_ME' > secrets/kafka/keystore_creds
printf 'CHANGE_ME' > secrets/kafka/key_creds
printf 'CHANGE_ME' > secrets/kafka/truststore_creds
```

### Create Kafka SCRAM Users
```bash
# Run inside the Kafka broker container
docker exec -it broker bash
kafka-configs --bootstrap-server broker:29092 \
    --alter --add-config 'SCRAM-SHA-512=[password=CHANGE_ME]' \
    --entity-type users --entity-name app1
```

## 🐳 Docker Images

### Optimized Images
This setup uses optimized Docker images for better performance:

- **Multi-stage builds** - Reduced image sizes (~40% smaller)
- **Custom Airflow image** - Pre-installed dependencies for faster startup
- **Alpine variants** - PostgreSQL and HTTPD use Alpine Linux (~80% smaller)
- **Layer caching** - Optimized Dockerfile layer ordering

### Building Custom Images
```bash
# Build all images
docker compose build

# Build specific service
docker compose build python-master
docker compose build apiserver scheduler dag-processor
```

## 📊 Logging

### Broker Logging Configuration
Broker logging is automatically configured to ERROR level to reduce log verbosity. The configuration is handled by `configure_broker_logging.sh` during setup.

To manually reconfigure:
```bash
bash configure_broker_logging.sh
```

Log configuration: `secrets/kafka/log4j.properties`

## 🔄 Reverse Proxy Configuration

The reverse proxy (Apache HTTPD) is available in dev profile. Configure it in `conf/httpd_conf/httpd.conf`:

### Confluent/Kafka Control Center
```apache
ProxyPass        /dist/           http://control-center:9021/dist/
ProxyPassReverse /dist/           http://control-center:9021/dist/
ProxyPass        /static/         http://control-center:9021/static/
ProxyPassReverse /static/         http://control-center:9021/static/
ProxyPass        /manifest.json   http://control-center:9021/manifest.json
ProxyPassReverse /manifest.json   http://control-center:9021/manifest.json
ProxyPass        /favicon.ico     http://control-center:9021/favicon.ico
ProxyPassReverse /favicon.ico     http://control-center:9021/favicon.ico
ProxyPass        /2.0/            http://control-center:9021/2.0/
ProxyPassReverse /2.0/            http://control-center:9021/2.0/
ProxyPass        /3.0/            http://control-center:9021/3.0/
ProxyPassReverse /3.0/            http://control-center:9021/3.0/
```

### Airflow
- Use root path (`/`) for Airflow. Static assets do not proxy well through Apache httpd.

### Spark
```apache
ProxyPass        /spark/  http://spark-master:8080/
ProxyPassReverse /spark/  http://spark-master:8080/
```

## 🛠️ Common Commands

### Start services
```bash
docker compose up -d
```

### Stop services
```bash
docker compose down
```

### View logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f broker
docker compose logs -f apiserver
```

### Airflow database migration
```bash
docker compose run --rm apiserver airflow db migrate
```

### Access containers
```bash
# Python master container
docker compose exec python-master bash

# Kafka broker
docker compose exec broker bash

# Airflow scheduler
docker compose exec scheduler bash
```

### Check service status
```bash
docker compose ps
```

## 📝 Notes

- **Replace all placeholder passwords** (`CHANGE_ME`, `kafka.example.com`) with production-safe values
- **Keep secrets out of version control** - Use your CI/secret manager for deployments
- **Environment variables** - Copy `env_example.txt` to `.env` and configure
- **Secrets directory** - Never commit `./secrets/` directory to git
- **Image optimization** - Custom images are built with dependencies pre-installed for faster startup

## 🔍 Troubleshooting

### Services won't start
1. Check logs: `docker compose logs <service-name>`
2. Verify environment variables in `.env`
3. Ensure secrets are properly configured in `./secrets/kafka/`

### Airflow database issues
```bash
# Reset database (WARNING: deletes all data)
docker compose run --rm apiserver airflow db reset

# Migrate database
docker compose run --rm apiserver airflow db migrate
```

### Kafka connection issues
1. Verify broker is healthy: `docker compose ps broker`
2. Check broker logs: `docker compose logs broker`
3. Verify SSL certificates are in place
4. Check network connectivity: `docker compose exec broker ping zookeeper`

### Port conflicts
If ports are already in use, modify `docker-compose.yaml` to use different ports.

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Confluent Platform Documentation](https://docs.confluent.io/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

[Add your license here]
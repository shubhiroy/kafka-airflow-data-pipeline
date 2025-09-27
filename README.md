# 🚀 Real-Time Data Engineering Pipeline

A comprehensive data engineering pipeline built with **Apache Kafka**, **Apache Spark**, **Apache Cassandra**, and **Apache Airflow** for real-time data streaming, processing, and storage.

![System Architecture](https://github.com/shubhiroy/kafka-airflow-data-pipeline/blob/main/docs/architecture.png)

## 📋 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project demonstrates a complete real-time data engineering pipeline that:

1. **Ingests** real-time user data from Random User API
2. **Streams** data through Apache Kafka
3. **Processes** data using Apache Spark Streaming
4. **Stores** processed data in Apache Cassandra
5. **Orchestrates** workflows with Apache Airflow
6. **Monitors** the entire pipeline through various dashboards

## 🏗️ System Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Random    │    │   Apache    │    │   Apache    │    │   Apache    │    │  Apache     │
│   User API  │───▶│   Airflow   │───▶│    Kafka    │───▶│    Spark    │───▶│  Cassandra  │
│             │    │             │    │             │    │  Streaming  │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │                    │                    │
                           ▼                    ▼                    ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │ PostgreSQL  │    │   Schema    │    │   Control   │
                   │ (Metadata)  │    │  Registry   │    │   Center    │
                   └─────────────┘    └─────────────┘    └─────────────┘
```

### Components:

- **Data Source**: Random User API generating synthetic user data
- **Orchestration**: Apache Airflow for workflow management
- **Message Broker**: Apache Kafka for real-time data streaming
- **Stream Processing**: Apache Spark for data transformation
- **Data Storage**: Apache Cassandra for scalable data persistence
- **Schema Management**: Confluent Schema Registry
- **Monitoring**: Kafka Control Center
- **Containerization**: Docker & Docker Compose

## 🛠️ Technologies Used

| Technology | Version | Purpose |
|------------|---------|---------|
| **Apache Kafka** | 7.4.0 | Message streaming platform |
| **Apache Spark** | 3.4.1 | Distributed data processing |
| **Apache Cassandra** | Latest | NoSQL database |
| **Apache Airflow** | 2.6.0 | Workflow orchestration |
| **Python** | 3.9 | Programming language |
| **Docker** | Latest | Containerization |
| **PostgreSQL** | 14.0 | Airflow metadata store |

## ✨ Features

- 🔄 **Real-time Data Streaming**: Continuous data ingestion from external APIs
- ⚡ **Fault-tolerant Processing**: Spark streaming with checkpointing
- 📊 **Scalable Storage**: Cassandra for high-availability data storage
- 🎛️ **Workflow Management**: Airflow DAGs for pipeline orchestration
- 🐳 **Containerized Deployment**: Full Docker Compose setup
- 📈 **Monitoring & Observability**: Built-in monitoring dashboards
- 🔧 **Environment-aware Configuration**: Easy local/production switching

## 📋 Prerequisites

Before running this project, ensure you have:

- **Docker** and **Docker Compose** installed
- **Python 3.9+** (for local development)
- **Git** for version control
- **8GB+ RAM** recommended for all services
- **Ports available**: 8080, 8081, 9021, 9042, 9090, 9092, 2181

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/shubhiroy/kafka-airflow-data-pipeline.git
cd kafka-airflow-data-pipeline
```

### 2. Start the Pipeline
```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 3. Access the Dashboards
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Kafka Control Center**: http://localhost:9021
- **Spark Master**: http://localhost:9090

### 4. Run the Streaming Pipeline
```bash
# Activate virtual environment (optional for local development)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run Spark streaming job
python spark_stream.py
```

## 📁 Project Structure

```
kafka-airflow-data-pipeline/
├── dags/                          # Airflow DAGs
│   └── kafka_stream.py           # Data ingestion DAG
├── script/                        # Setup scripts
│   └── entrypoint.sh             # Airflow initialization
├── logs/                          # Application logs
├── plugins/                       # Airflow plugins
├── docker-compose.yml             # Multi-service orchestration
├── spark_stream.py               # Spark streaming application
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git ignore rules
└── README.md                     # This file
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file for local development:

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Cassandra Configuration  
CASSANDRA_HOST=localhost

# Airflow Configuration
AIRFLOW_UID=1000
AIRFLOW_GID=0
```

### Docker Services Configuration

Key services and their ports:

| Service | Port | Description |
|---------|------|-------------|
| Kafka Broker | 9092 | Kafka bootstrap server |
| Zookeeper | 2181 | Kafka coordination |
| Schema Registry | 8081 | Schema management |
| Control Center | 9021 | Kafka monitoring |
| Airflow Webserver | 8080 | Workflow UI |
| Spark Master | 9090 | Spark cluster UI |
| Cassandra | 9042 | Database connection |

## 📖 Usage

### 1. Data Ingestion
The pipeline automatically ingests data from the Random User API every day via Airflow:

```python
# Airflow DAG: dags/kafka_stream.py
- Fetches user data from randomuser.me API
- Formats and enriches data
- Publishes to Kafka topic 'user_data'
```

### 2. Stream Processing
Spark processes the streaming data in real-time:

```python
# Spark Streaming: spark_stream.py  
- Consumes from Kafka topic
- Applies transformations and schema validation
- Writes to Cassandra with fault tolerance
```

### 3. Data Storage
Data is stored in Cassandra with the following schema:

```sql
CREATE TABLE spark_streams.users (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    email TEXT,
    username TEXT,
    dob TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
```

### 4. Query Data
Connect to Cassandra to query processed data:

```bash
# Connect to Cassandra
docker exec -it cassandra cqlsh -u cassandra -p cassandra

# Query data
USE spark_streams;
SELECT * FROM users LIMIT 10;
```

## 🔄 Data Flow

1. **API Data Ingestion** → Airflow scheduler triggers DAG daily
2. **Data Formatting** → Python transforms raw API response  
3. **Kafka Publishing** → Formatted data sent to Kafka topic
4. **Stream Processing** → Spark consumes and processes data
5. **Data Validation** → Schema validation and data quality checks
6. **Persistent Storage** → Processed data written to Cassandra
7. **Monitoring** → Track pipeline health via dashboards

## 📊 Monitoring

### Airflow Dashboard
- Monitor DAG execution status
- View task logs and dependencies
- Manage pipeline schedules

### Kafka Control Center  
- Topic management and monitoring
- Consumer lag tracking
- Schema registry management

### Spark UI
- Job execution monitoring
- Resource utilization tracking
- Streaming statistics

## 🐛 Troubleshooting

### Common Issues

**1. Kafka Connection Issues**
```bash
# Check Kafka broker health
docker-compose logs kafka

# Verify topic creation
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

**2. Spark Streaming Errors**
```bash
# Check Spark logs
docker-compose logs spark-master spark-worker

# Verify Cassandra connection
docker exec -it cassandra cqlsh -u cassandra -p cassandra
```

**3. Airflow DAG Issues**
```bash
# Check Airflow logs
docker-compose logs webserver scheduler

# Test DAG manually
docker exec -it webserver airflow dags test user_automation 2025-09-27
```

### Performance Tuning

- **Kafka**: Adjust `num.partitions` and `replication.factor`
- **Spark**: Configure `spark.sql.streaming.trigger` interval
- **Cassandra**: Optimize `replication_factor` for your cluster

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Apache Software Foundation](https://apache.org/) for the amazing open-source tools
- [Confluent](https://confluent.io/) for Kafka ecosystem
- [Random User API](https://randomuser.me/) for test data generation

---

## 🔗 Links

- **Repository**: https://github.com/shubhiroy/kafka-airflow-data-pipeline
- **Documentation**: [Wiki](https://github.com/shubhiroy/kafka-airflow-data-pipeline/wiki)
- **Issues**: [GitHub Issues](https://github.com/shubhiroy/kafka-airflow-data-pipeline/issues)

---

**Built with ❤️ by [Shubham Roy](https://github.com/shubhiroy)**
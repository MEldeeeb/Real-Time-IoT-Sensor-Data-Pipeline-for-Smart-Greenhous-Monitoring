# Real-Time AgriTech Data Pipeline for Smart Greenhouses

---

## Description

**Real-Time AgriTech Data Pipeline for Smart Greenhouses** powered by an end-to-end real-time **data pipeline**. The system simulates IoT sensor data, including temperature, humidity, air quality, and soil moisture, using **Python** to reflect realistic greenhouse conditions.  
Each sensor streams data into a unified **Apache Kafka** topic, with producers and consumers handling ingestion and initial processing. Incoming data is **validated and enriched** with metadata before being stored in an **Operational Data Store (ODS)** using **PostgreSQL** for real-time monitoring. 
For historical insights and strategic decision-making:
- Processed data is modeled in a **star schema** and loaded into **Snowflake** (EDWH).
- Raw data is archived in **HDFS** to support batch processing.
- **Apache Spark** runs nightly aggregation jobs to analyze daily sensor trends and generate **automated email reports** for stakeholders.

---

## System Architecture

![System Architecture](/images/Project architecture.png)

---

## Logical Components

### 1. Data Sources

**IoT Sensors**  
Devices that generate real-time data (temperature, humidity, air quality, soil moisture).

- **Input Format:** JSON

```json
{
  "sensor_id": "string",
  "timestamp": "string",
  "sensor_type": "string",
  "value": float,
  "location": "string"
}
```

---

## Ingestion Layer

**Apache Kafka**
- Acts as the backbone of real-time data ingestion and messaging.
- Each sensor type writes to a dedicated Kafka topic (e.g., `temperature`, `air_quality`, `humidity`).

---

## Processing Layer

**Real-Time Processing**
- **Python Kafka Consumer:**
  - Reads messages from Kafka topics.
  - Validates and enriches data with metadata (e.g., standardized timestamps, location codes).

**Batch Processing**
- **Apache Spark on HDFS:**
  - Runs nightly batch jobs to aggregate sensor data by:
    - Region
    - Sensor type
    - Time intervals (daily, weekly)
  - Generates reports and summary tables.

---

## Storage Layer

- **Data Lake (Raw Data):**
  - **HDFS** archives unprocessed IoT sensor data for:
    - Historical analysis
    - Reprocessing scenarios

- **Data Warehouse (Processed Data):**
  - **Snowflake:** Stores cleaned, modeled data for analytics and dashboards.
  - **PostgreSQL:** Stores current real-time data for monitoring applications.

---

## Analytics, Visualization, & Reporting

- **Visualization:**
  - Real-time KPIs like active sensors and current readings.
  - Historical trends and aggregated metrics.
  - **Power BI** dashboards for exploration and reporting.

- **Reporting:**
  - Daily automated email reports to stakeholders with summarized sensor trends and anomalies.

---

## Schema

![Schema](attachment:e0e07095-2bc1-4d79-b4d4-7fa07960288b:Schema.png)

---

## Future Work

- **Docker** containerization of all services to improve:
  - Deployment consistency
  - Scalability
  - Portability
- Full **workflow orchestration with Apache Airflow** to automate:
  - Spark aggregation jobs
  - ETL pipelines (ODS → Snowflake)
  - Report generation and distribution

---

## High-Level Data Flow

1. **Data Generation:**
   - IoT sensors send data as JSON messages to Kafka.
2. **Ingestion:**
   - Kafka brokers distribute data across topics and partitions.
3. **Processing:**
   - Python consumers transform and validate data in real time.
   - Spark jobs perform daily batch aggregations.
4. **Storage:**
   - Raw data is archived in HDFS.
   - Cleaned data is saved to PostgreSQL and Snowflake.
5. **Visualization:**
   - Power BI dashboards fetch insights from the warehouse.
6. **Reporting:**
   - Automated emails deliver daily summaries to stakeholders.

---

# News Data Analysis with Event-Driven Incremental Load in Snowflake Table
This project demonstrates an end-to-end data pipeline that ingests real-time news data from a public API, stores it in Google Cloud Storage (GCS), and performs incremental and event-driven loading into a Snowflake table using Apache Airflow. The pipeline showcases modern data engineering practices and integration between cloud services.

## 📌 Objective
The objective of this project is to:
- Collect news data programmatically using Python from a public API.
- Store the raw data in **Parquet** format in **Google Cloud Storage (GCS)**.
- Use **Apache Airflow** to orchestrate the workflow.
- Automatically create or update tables in **Snowflake** using event-driven incremental logic.
- Ensure the entire pipeline is modular, efficient, and production-ready.

## 🧰 Tools & Technologies Used
- **Python** – For API calls and scripting
- **NewsAPI.org** – Public news data source
- **Google Cloud Storage (GCS)** – For storing raw data files
- **Apache Airflow (GCP Composer)** – For orchestration and scheduling
- **Snowflake** – Cloud Data Warehouse to store final structured data
- **SQL** – For table creation and data loading in Snowflake
- **Parquet Format** – For efficient storage and retrieval

## 🧩 Project Structure
```plaintext
News-Data-Analysis-Project/
│
├── fetch_news.py                  # Python script to fetch news and store as Parquet
├── news_api_airflow_job.py       # Airflow DAG to automate the pipeline
├── snowflake_commands.sql        # SQL script for table creation and schema inference
└── README.md                      # Project documentation (this file)

Workflow Overview

Data Ingestion:
fetch_news.py connects to the News API.
Fetches top headlines or category-based articles.
Converts them into a Pandas DataFrame.
Saves the data as a .parquet file to a GCS bucket.

Airflow Orchestration:
A DAG (news_api_airflow_job.py) triggers the ingestion and loading process.
First, it uploads the new .parquet file to GCS.
Then it runs SQL to infer schema and load the data incrementally into Snowflake.

Snowflake Loading:
A CREATE TABLE IF NOT EXISTS ... USING TEMPLATE statement is used.
It auto-creates schema based on the uploaded .parquet file.

Learnings
Integration of Airflow with external cloud services like Snowflake and GCS.
Dynamic schema inference using INFER_SCHEMA in Snowflake.
Handling connections and secrets in Airflow using conn_id.
Automating real-time data pipelines using DAGs and SQL scripts.

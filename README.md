# 📈 Airflow Stock Market Data Pipeline

## **Overview**
This project implements an **ETL data pipeline** using **Apache Airflow** (Astro CLI) to automate the extraction, transformation, and loading of stock market data into a PostgreSQL database for analytics and visualization.

The pipeline runs inside a **Dockerized environment** and uses **Metabase** as a BI tool to create interactive dashboards.


## **Architecture**
   The Arch--> Architecture/Airflow_Arch_Project.png

## **Pipeline Workflow**
Extract stock market data from API.

Transform using Python & Pandas (clean, validate, format).

Load into PostgreSQL.

Visualize in Metabase dashboards.

## **Tech Stack**
Apache Airflow (Astro CLI) – Workflow orchestration

Python – Data extraction & transformation

PostgreSQL – Data warehouse

Metabase – Visualization

Docker – Containerization

Minio  - Datalake

Spark – Data processing

## **Setup Instructions**
1. Clone the repository

git clone https://github.com/abdelrhmanmousa/Airflow-Stock-Pipeline.git
cd Airflow-Stock-Pipeline

2. Start Astro/Airflow Environment
astro dev start

4. Verify Airflow UI
Open http://localhost:8080

Default login: admin / admin

4. Verify PostgreSQL
Host: postgres

Port: 5432

User: postgres

Password: postgres

5. Access Metabase Dashboard
Open http://localhost:3000

Connect to PostgreSQL with above credentials.

Running the DAG
Run manually for a specific date:

astro dev run dags test stock_market 2025-08-01
Or enable scheduling in Airflow UI.

## **Dashboard Example**
Once data is loaded, Metabase can create:

Daily Stock Price Trends

Volume Analysis

Top Gainers/Losers


## **Folder Structure**

.
├── Architecture/
├── dags/                  # Airflow DAGs
│   └── stock_market.py
├── include/               # Helper scripts
├── plugins/               # Custom Airflow plugins
├── docker-compose.override.yml
├── Dockerfile
├── requirements.txt
└── README.md

## **Future Improvements**
Add Kafka for real-time streaming.

Integrate more APIs (crypto, forex, etc.).


# 🏏 IPL ETL Pipeline & Visualization Project

## 📌 Overview

This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline built using an IPL dataset to showcase practical data engineering and analytics skills.

It includes:
- Data ingestion with **Hadoop**
- Data transformation using **PySpark**
- Workflow orchestration via **Apache Airflow**
- Data storage in **PostgreSQL**
- Interactive dashboards with **Power BI**

## 🧰 Technologies & Tools

- **Hadoop (HDFS)** – for storing raw IPL data  
- **PySpark** – for data transformation and cleansing  
- **Apache Airflow** – to schedule and automate ETL workflows  
- **PostgreSQL** – for storing the processed data  
- **pgAdmin4** – for database management  
- **Power BI** – for data visualization  
- **CentOS 9 on Oracle VirtualBox** – to simulate a real-world Linux environment

## 🔄 ETL Pipeline Workflow

1. **Extract**: Load raw IPL CSV files into HDFS using Hadoop.
2. **Transform**: Clean and structure data using PySpark.
3. **Load**: Insert transformed data into PostgreSQL.
4. **Orchestrate**: Automate tasks with Apache Airflow DAGs.
5. **Visualize**: Build dynamic dashboards in Power BI for insights.

## 📊 Key Features

- End-to-end data engineering workflow simulation
- Automation with Apache Airflow
- Real-world environment deployment on CentOS 9
- Clean and structured data models
- Interactive data visualizations highlighting key IPL stats (top scorers, match outcomes, team performance, etc.)

## 📁 Project Structure

project-root/ ├── data/ # Raw IPL dataset files(raw_data folder) ├── hadoop/ # Scripts for HDFS operations ├── pyspark_jobs/ # PySpark scripts for ETL ├── airflow/ # Airflow DAGs and config ├── sql/ # SQL scripts for PostgreSQL setup ├── dashboards/ # Power BI files └── README.md # Project overview


## 🚀 Getting Started

### Prerequisites

- Hadoop
- Spark with PySpark
- Apache Airflow
- PostgreSQL with pgAdmin4
- Power BI (Desktop)
- CentOS 9 (or any Linux VM)

### Steps

1. Clone this repo
2. Set up Hadoop and load raw data into HDFS
3. Run PySpark jobs for transformation
4. Set up and run Airflow DAGs to automate the pipeline
5. Load data into PostgreSQL
6. Connect Power BI to PostgreSQL and create dashboards

## 📈 Sample Dashboard

*(Include a screenshot or link to your Power BI dashboard here if possible)*

## 🧑‍💻 Author

**Your Name**  
*Aspiring Data Engineer | Passionate about building data pipelines & insights*  
[LinkedIn](#) • [GitHub](#)

## 📄 License

This project is licensed under the MIT License.

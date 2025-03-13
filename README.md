# 📚 Library Management System

This repository contains the **Library Management System** project, which leverages **Azure Databricks** for efficient data transformation and analytics. It follows a structured data pipeline approach with **incremental data loading**, ensuring optimized and up-to-date data processing.

## 🔄 Incremental Data Processing

The project follows an **incremental data loading strategy**, where new and updated records are processed instead of reloading entire datasets. This enhances efficiency, reduces computation costs, and ensures real-time analytics.

### 🔹 Data Pipeline Workflow:
1. **Bronze Layer (Raw Data Ingestion)**
   - Ingests raw data from **Azure Data Lake Storage (ADLS Gen2)**.
   - Stores unprocessed records for historical tracking.

2. **Silver Layer (Data Cleaning & Standardization)**
   - Cleans and transforms raw data using **PySpark**.
   - Standardizes formats, removes duplicates, and handles missing values.
   - **Incrementally loads new data**, ensuring up-to-date processing.

3. **Gold Layer (Aggregated & Enriched Data)**
   - Performs aggregations and business logic transformations.
   - Prepares final datasets for **reporting, analytics, and dashboards**.
   - Supports **incremental updates** to maintain optimized query performance.

4. **Dashboards (Visualization & Insights)**
   - Built-in **Databricks SQL Dashboards** for real-time analytics.
   - Provides insights into **book transactions, student activity, delays, and fines**.

## 🏗️ Project Structure

The repository is organized as follows:

- **Silver/** - Cleaned and transformed data from the Bronze layer.
- **Gold/** - Aggregated and enriched data for analytics and reporting.
- **Dashboards/** - Notebooks and scripts for visualizing library data.
- **Datasets/** - Raw and processed datasets used in the project.
- **Media/** - Contains all multimedia assets:
  - **Images/** - Screenshots, diagrams, and visuals.
  - **Video/** - Recordings of project demos, tutorials, and presentations.

## 🚀 Technologies Used
- **Azure Data Factory (ADF)** – For data orchestration and scheduling.
- **Azure Databricks** – For data processing and transformations.
- **ADLS Gen2** – Cloud storage for raw and processed data.
- **PySpark** – Data transformation and incremental loading.
- **Databricks Dashboard** – Analytics and visualization.
- **Delta Lake** – Optimized storage for incremental data processing.
- **Unity Catalog** – For data governance and security with access control policies.

## 🎯 Future Enhancements
- Implementing **Machine Learning models** for book recommendations.
- Enhancing **real-time analytics** with streaming data ingestion.

This project enables **efficient library management** by leveraging **incremental data loading, optimized transformations, and real-time analytics**. 🚀📊

# 🚖 NYC Taxi Data Engineering Project

## 📌 Overview

This project demonstrates an end-to-end data engineering pipeline built using Azure Data Lake, Azure Databricks, Delta Lake, and Power BI. The pipeline processes raw NYC taxi data and transforms it into analytics-ready datasets for business insights.

---

## 🏗️ Architecture

The project follows the **Medallion Architecture (Bronze → Silver → Gold)**:

* **Bronze Layer**: Raw CSV data ingested into Azure Data Lake
* **Silver Layer**: Data cleaning, transformation, and schema enforcement using Databricks (PySpark)
* **Gold Layer**: Aggregated and business-ready data stored as Delta tables

> 📷 Add your architecture diagram here
> ![Architecture](./architecture.png)
<img width="1536" height="1024" alt="f38440c6-9cb1-4474-9cb2-4ed6f7a02052" src="https://github.com/user-attachments/assets/d03b5fe0-b2a5-402a-8590-d00114c5717a" />

---

## ⚙️ Tech Stack / Services Used

* Azure Data Lake Storage Gen2
* Azure Databricks
* Apache Spark (PySpark)
* Delta Lake
* Power BI

---

## 🔄 Data Pipeline Flow

1. **Ingestion (Bronze)**
   Raw NYC taxi data is stored in Azure Data Lake

2. **Transformation (Silver)**
   Data is cleaned, structured, and validated using PySpark in Databricks

3. **Aggregation (Gold)**
   Data is aggregated and stored in Delta format for analytics

4. **Visualization**
   Power BI connects to Databricks and creates interactive dashboards

---

## 📁 Project Structure

```
nyc-databricks-powerbi-project/
│
├── notebooks/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│
├── powerbi/
│   └── dashboard.pbix
│
├── README.md
```

---

## 🚀 Key Features

* End-to-end data pipeline (Bronze → Silver → Gold)
* Data transformation using PySpark
* Delta Lake implementation for reliability and performance
* Managed & External table handling in Databricks
* Integration with Power BI for visualization

---

## 📊 Power BI Dashboard

> 📷 Add your dashboard screenshots here
> ![Dashboard](./dashboard.png)

Key insights:

* Trip type distribution
* Fare analysis
* Distance vs passenger trends

---

## 📚 Learnings

* Difference between Delta Lake and Parquet
* Managed vs External tables in Databricks
* Data pipeline architecture (Medallion Architecture)
* Connecting Databricks with Power BI using Access Token
* Handling large-scale data processing with Spark

---

## ▶️ How to Run

1. Upload raw data to Azure Data Lake (Bronze layer)
2. Run Databricks notebooks for transformation
3. Create Delta tables in Silver and Gold layers
4. Connect Power BI using Server Hostname, HTTP Path, and Access Token
5. Build dashboards

---

## ⚠️ Note

Sensitive information like access tokens, client secrets, and credentials have been removed for security purposes.

---

## 🌟 Conclusion

This project showcases a real-world implementation of a modern data engineering pipeline using industry-standard tools and best practices.

---

# 🔥 Apache Spark Data Engineering Portfolio

> **End-to-end PySpark solutions demonstrating production-grade data engineering patterns**

[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)](https://spark.apache.org)
[![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)](https://azure.microsoft.com)

---

## 📋 Overview

This portfolio showcases hands-on Apache Spark data engineering projects covering large-scale data processing, optimization techniques, and real-world pipeline patterns. Built to demonstrate enterprise-level skills aligned with modern data engineering roles.

---

## 🛠️ Projects & Topics Covered

### 1. ⚡ Local Sort vs. Global Sort in Spark
- Comparative analysis of `sortWithinPartitions()` vs `sort()`
- Performance benchmarking and shuffle optimization
- Use cases for each sorting strategy in production pipelines

### 2. 🔄 ETL Pipeline Patterns
- Incremental data loading strategies
- Schema evolution handling
- Data quality checks and validation layers

### 3. 📊 Spark SQL & Analytics
- Window functions for time-series analysis
- Aggregation optimization with partition pruning
- Joins: broadcast, shuffle hash, sort merge

### 4. ☁️ Cloud Integration
- Azure Data Lake Storage Gen2 (ADLS) integration
- Delta Lake for ACID transactions
- Databricks-compatible notebook patterns

---

## 🧰 Tech Stack

| Category | Technologies |
|----------|-------------|
| Processing | Apache Spark (PySpark), Spark SQL |
| Storage | Azure Data Lake Storage, Delta Lake, Parquet |
| Orchestration | Apache Airflow |
| Cloud | Microsoft Azure |
| Languages | Python, SQL |

---

## 📁 Repository Structure

```
Apache-Spark-Data-Engineering-Portfolio/
├── sorting/
│   ├── local_sort_vs_global_sort.ipynb    # Sort benchmarking
│   └── partition_optimization.py
├── etl/
│   ├── incremental_load_pattern.py         # Incremental ETL
│   └── schema_evolution_handler.py
├── sql/
│   ├── window_functions.sql                # Advanced SQL
│   └── aggregation_patterns.ipynb
└── cloud/
    └── azure_adls_integration.py           # Cloud integration
```

---

## 🚀 Key Takeaways

- **Scalability**: Designed to handle TB-scale datasets in distributed environments
- **Performance**: Optimized with partition strategies, caching, and broadcast joins
- **Production-Ready**: Error handling, logging, and monitoring patterns included

---

## 📫 Connect

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/fode-guirassy-83b089229/)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat-square&logo=github&logoColor=white)](https://github.com/GuirassyFode)

# 📦 DBT ETL (PostgreSQL + dbt)

### End-to-End Data Engineering | Medallion Architecture | ML-Ready Feature Store

This project implements a fully engineered **Medallion architecture (Bronze → Silver → Gold)** using **PostgreSQL**, **dbt**, and **VS Code**, transforming multi-source CRM/ERP datasets into a clean analytics-ready and ML-ready dataset.

The pipeline loads raw CSVs, performs cleaning + validation, and produces a **final gold fact table** designed for machine learning tasks such as forecasting, segmentation, regression and customer behaviour modelling.

---

# 🧱 Architecture Overview

## 🥉 Bronze Layer — Raw Data
- Raw CSVs stored in `seeds/bronze/`
- Loaded into PostgreSQL using `dbt seed`
- Stored **exactly as received**
- Ensures reproducibility + acts as the system of record

## 🥈 Silver Layer — Cleaned & Conformed
- Cleaned models for:
  - `customers`
  - `products`
  - `sales_clean`
- Applies:
  - Data standardisation  
  - Missing value handling  
  - Type casting  
  - Date corrections  
  - Product hierarchy enrichment  
  - Customer gender/location standardisation  

## 🥇 Gold Layer — Machine Learning Feature Store
- A fully joined **fact_sales** table containing:
  - Customer attributes  
  - Product attributes  
  - Sales metrics  
  - Time features  
  - Behavioural metrics (recency, frequency, monetary value)  
- Optimised for:
  - Forecasting  
  - Clustering  
  - Segmentation  
  - Regression  
  - Classification  
  - Recommendation systems  

---

# 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **PostgreSQL** | Database + medallion schemas |
| **dbt Core** | Transformations, orchestration, lineage & documentation |
| **VS Code** | Development |
| **pgAdmin** | SQL validation & monitoring |
| **Python** | ML experiments (outside dbt) |

---

# ⚙️ Pipeline Runbook

This project uses **multiple dbt targets** so each layer builds into its own schema:
## ✔ 1. Seed the Bronze layer

```bash
dbt seed -t bronze
```
## ✔ 2. Build the Silver layer (cleaned views)
```bash
dbt run -t silver --select silver
```
## ✔ 3. Build the Gold layer (ML fact table)
```bash
dbt run -t gold --select gold
```
---

# 📁 Project Structure
```
DBT-Medallion_Pipeline/
│
├── models/
│   ├── bronze/        → raw seed sources  
│   ├── silver/        → cleaned conformed models  
│   └── gold/          → ML-ready fact table  
│
├── seeds/
│   └── bronze/        → raw CSVs  
│
├── macros/            → optional macros  
├── dbt_project.yml
└── README.md
```
---

# 📊 dbt Documentation

Generate docs:
```bash
dbt docs generate
```

Serve docs locally:
```bash
dbt docs serve
```

This provides:
- Interactive DAG
- Column-level lineage
- Model descriptions
- Source documentation
- Schema browser

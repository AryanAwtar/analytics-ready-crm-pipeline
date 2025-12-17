
# Enterprise CRM Data Engineering & Analytics Pipeline

## Overview
This project implements an **end-to-end data engineering and analytics pipeline** that transforms raw, inconsistent CRM-style data into clean, analysis-ready datasets and actionable business insights.

The pipeline is designed to mirror real-world enterprise data workflows, including ingestion, data quality checks, deduplication, enrichment, transformation, and analytics-ready exports. All data used is **synthetic or public**, making the project safe for open-source sharing while preserving real production complexity.

---

## Objectives
- Build a scalable ETL pipeline for CRM-style datasets
- Improve data quality through validation, deduplication, and enrichment
- Generate business-facing analytics and KPIs
- Produce CRM-ready outputs for downstream systems
- Demonstrate production-style data engineering practices

---

## Key Features
- Multi-stage ETL pipeline
- Schema detection and column normalization
- Duplicate detection and removal
- Address and attribute enrichment using lookup data
- Entity (owner/customer) normalization and matching
- Pricing and segmentation logic
- Data quality metrics and pipeline statistics
- SQL-ready exports for analytics and reporting

---

## Project Architecture
```

enterprise-crm-pipeline/
│
├── src/
│   ├── ingestion.py          # Raw data loading and schema alignment
│   ├── cleaning.py           # Missing values, normalization, deduplication
│   ├── enrichment.py         # Address and attribute enrichment
│   ├── entity_resolution.py  # Owner / entity normalization and matching
│   ├── pricing.py            # Pricing and segmentation logic
│   ├── metrics.py            # Data quality and pipeline metrics
│
├── data/
│   ├── raw/                  # Synthetic raw CRM-style data
│   └── processed/            # Cleaned and enriched outputs
│
├── sql/
│   └── analytics_queries.sql # SQL queries for KPI analysis
│
├── reports/
│   ├── kpi_summary.csv
│   └── pipeline_metrics.csv
│
├── scripts/
│   └── run_pipeline.py       # Pipeline execution entry point
│
└── README.md

```

---

## Data Pipeline Flow
1. **Ingestion**
   - Load raw CRM-style data files
   - Detect schema and align columns

2. **Data Cleaning**
   - Handle missing and invalid values
   - Normalize text fields
   - Remove duplicate records

3. **Data Enrichment**
   - Enrich missing attributes using lookup tables
   - Standardize address components

4. **Entity Resolution**
   - Normalize owner/customer names
   - Apply fuzzy matching for entity grouping
   - Exclude unwanted entity categories via rules

5. **Pricing & Segmentation**
   - Compute pricing variables
   - Create segmentation buckets

6. **Metrics & Validation**
   - Track row counts across stages
   - Measure data quality improvements
   - Log pipeline execution statistics

7. **Exports**
   - Generate analytics-ready datasets
   - Produce SQL-compatible outputs

---

## Key Metrics Generated
- Duplicate record percentage
- Missing value resolution rate
- Records processed per stage
- Pricing and segmentation distributions
- Data quality score per record (optional)

---

## Technologies Used
- Python
- Pandas, NumPy
- SQL (MySQL / SQLite)
- Fuzzy matching libraries
- CSV / JSON data formats
- Logging and metrics tracking

---

## Sample Use Cases
- CRM data cleanup and standardization
- Business analytics preparation
- Data quality assessment
- Portfolio or customer segmentation
- Analytics pipeline prototyping

---

## Disclaimer
All datasets used in this project are synthetic or publicly available.  
This project does not use or expose any proprietary, confidential, or production data.

---

## Author
**Aryan Raj**  
Data Analyst | Data Engineering | Automation  

- GitHub: https://github.com/AryanAwtar  
- LinkedIn: https://www.linkedin.com/in/aryan-raj-39a8b61b1/

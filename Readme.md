# PII Detection & Data Quality Validation Pipeline

A Python-based data engineering pipeline for detecting personally identifiable information (PII), validating data quality, and remediating issues in customer datasets.

## 🎯 Project Overview

This mini-project demonstrates real-world data engineering skills:
- Data profiling and quality assessment
- PII detection and masking
- Data validation and cleaning
- ETL pipeline orchestration
- Compliance and governance thinking

## 📊 Dataset

**Input**: `customers_raw.csv` - 10 customer records with quality issues
- Missing values
- Inconsistent formats (phone, dates)
- Invalid data
- PII exposure risk

## 🚀 Pipeline Stages

### ✅ Part 1: Exploratory Data Quality Analysis
**Status**: COMPLETE  
**Output**: `reports/data_quality_report.txt`

Profiles raw data to identify:
- Completeness (missing values %)
- Data type mismatches
- Format inconsistencies
- Invalid values
- Severity assessment

### ⏳ Part 2: PII Detection
**Status**: In Progress  
**Output**: `reports/pii_detection_report.txt`

Detects personally identifiable information:
- Names, emails, phone numbers
- Addresses, dates of birth
- Risk assessment

### ⏳ Part 3: Data Validation
**Status**: Pending  
**Output**: `reports/validation_results.txt`

Applies validation rules to enforce schema constraints.

### ⏳ Part 4: Data Cleaning
**Status**: Pending  
**Output**: `data/customers_cleaned.csv`, `reports/cleaning_log.txt`

Normalizes formats and handles missing values.

### ⏳ Part 5: PII Masking
**Status**: Pending  
**Output**: `data/customers_masked.csv`, `reports/masked_sample.txt`

Masks sensitive data while preserving structure.

### ⏳ Part 6: End-to-End Pipeline
**Status**: Pending  
**Output**: `reports/pipeline_execution_report.txt`

Orchestrates all stages into automated workflow.

### ⏳ Part 7: Reflection & Governance
**Status**: Pending  
**Output**: `docs/reflection.md`

Critical analysis of data quality, privacy, and operations.

## 📁 Project Structure

```
PII-DATA-QUALITY-PIPELINE/
├── data/                    # Input/output datasets
├── reports/                 # Generated reports
├── src/                     # Source code
│   └── utils/              # Shared utilities
├── docs/                    # Documentation
├── README.md               # This file
└── requirements.txt        # Dependencies
```

## 🛠️ Tech Stack

- **Python 3.x**
- **pandas** - Data manipulation
- **numpy** - Numerical operations
- **regex** - Pattern matching (PII detection)
- **Pandera/Pydantic** - Data validation (Part 3)

## 🎓 Learning Objectives

- Real-world data quality challenges
- PII compliance (GDPR-relevant)
- Production pipeline design
- Error handling and logging
- Documentation and governance

## 📝 Usage

```bash
# Run individual parts
python src/part1_data_quality_analysis.py
python src/part2_pii_detection.py
# ... etc

# Run full pipeline (Part 6)
python src/part6_pipeline.py
```

## 🔒 Compliance Note

This project demonstrates PII handling for educational purposes. In production:
- Follow GDPR, CCPA, and relevant regulations
- Implement access controls
- Maintain audit trails
- Use secure storage and encryption

## 📧 Author

Data Engineering Mini Project - Learning by Building

---

**Next Steps**: Complete Parts 2-7 to build a production-ready data pipeline!
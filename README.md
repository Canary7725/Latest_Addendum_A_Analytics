# CMS Healthcare Data Standardization Pipeline

This project automatically finds and downloads the latest CMS Addendum A healthcare dataset, cleans and standardizes its fields, and produces validated Parquet outputs using Pandas and PySpark.

The pipeline includes:

- Configuration-driven source-to-target schema mapping
- APC code standardization
- Currency and numeric value conversion
- Explicit Spark data types and decimal precision
- Initial and standardized Parquet outputs
- Validation checks for codes and monetary fields

## Setup

```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```cmd
python main.py
```

The CMS source URL, input file type, skipped rows, and output directories are configured in `config.json`.



# ANAIS Helios Pipeline - Quick Start Guide

## Overview

The Helios pipeline creates analytical views from Staging data and optionally uploads them to SFTP.

## Prerequisites

**Must run Staging pipeline first!**

```bash
cd ../anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

This creates the Staging DuckDB database that Helios needs.

## Two Ways to Run the Pipeline

### Option 1: Local Export Only (No SFTP)

Exports CSV files locally without uploading to SFTP:

```bash
cd DBT/anais_helios

# 1. Install dependencies
uv sync

# 2. Run pipeline (local export only)
uv run run_local_with_sftp.py --env "local" --profile "Helios"
```

**What it does:**
- ✅ Copies tables from Staging database
- ✅ Runs DBT transformations
- ✅ Exports views to `output/helios/*.csv`
- ❌ No SFTP upload

**Output location:** `output/helios/`

### Option 2: With SFTP Upload (Automatic)

Exports CSV files and uploads them to SFTP server:

```bash
cd DBT/anais_helios

# 1. Setup SFTP credentials (one-time)
cat > .env << 'EOF'
SFTP_HOST="your.sftp.host"
SFTP_PORT=22
SFTP_USERNAME="your_username"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
EOF
chmod 600 .env

# 2. Install dependencies
uv sync

# 3. Run pipeline with SFTP
uv run run_local_with_sftp.py --env "local" --profile "Helios" --use-sftp
```

**What it does:**
- ✅ Copies tables from Staging database
- ✅ Runs DBT transformations
- ✅ Exports views to `output/helios/*.csv`
- ✅ **Uploads files to SFTP** (`/SCN_BDD/HELIOS/output/`)

**SFTP upload location:** `/SCN_BDD/HELIOS/output/`

## Output Files

The pipeline creates 3 analytical views:

| View Name | Output File | Description |
|-----------|-------------|-------------|
| `helios__missions` | `siicea_YYYYMMDD.csv` | Inspection mission analytics |
| `helios__sirec` | `sirec_YYYYMMDD.csv` | SIREC transformations |
| `helios__sivss` | `sivss_YYYYMMDD.csv` | SIVSS transformations |

**Dynamic filename generation:** Output filenames are based on the dates extracted from input files. For example, if the input `sa_sivss.csv` was named `SIVSS_SCN_20251007.csv` on SFTP, the output will be `sivss_20251007.csv`.

If no input date is found, the current date is used as fallback (e.g., `sivss_20250123.csv`).

## Pipeline Flow

```
Staging DuckDB
   └─ data/staging/duckdb_database.duckdb
       ↓ copy tables
Helios DuckDB
   └─ data/helios/duckdb_database.duckdb
       ↓ dbt transformations
Analytical Views
   ├─ helios__missions
   ├─ helios__sirec
   └─ helios__sivss
       ↓ export
Local CSV Files
   └─ output/helios/*.csv
       ↓ upload (if --use-sftp)
SFTP Server
   └─ /SCN_BDD/HELIOS/output/*.csv
```

## Verify Output

### Check Local Files

```bash
# List exported files
ls -lh output/helios/

# Expected files (dates from input files):
# siicea_20251020.csv
# sirec_20251026.csv
# sivss_20251007.csv

# Preview content
head -10 output/helios/siicea_*.csv
```

### Query Database

```bash
duckdb data/helios/duckdb_database.duckdb
```

```sql
-- Show all tables
SHOW TABLES;

-- Query analytical views
SELECT COUNT(*) FROM helios__missions;
SELECT * FROM helios__missions LIMIT 10;

-- Check source tables
SELECT COUNT(*) FROM sa_sirec;
```

### Verify SFTP Upload (if used)

```bash
# Connect to SFTP
sftp -i ~/.ssh/id_rsa username@sftp.host

# Navigate to output directory
cd /SCN_BDD/HELIOS/output

# List uploaded files (named from input file dates)
ls -lt sivss_*.csv sirec_*.csv siicea_*.csv

# Exit
exit
```

## Troubleshooting

### Staging Database Not Found

```
❌ Staging DuckDB not found at: ../anais_staging/data/staging/duckdb_database.duckdb
```

**Solution:** Run Staging pipeline first

```bash
cd ../anais_staging
uv run run_local_with_sftp.py --env "local" --profile "Staging"
```

### SFTP Connection Failed

```
❌ SFTP connection failed: Authentication failed
```

**Solution:**

```bash
# Check .env file exists
ls -la .env

# Verify SSH key permissions
chmod 600 ~/.ssh/id_rsa
chmod 600 .env

# Test SSH connection manually
ssh -i ~/.ssh/id_rsa username@sftp.host

# Check logs
cat logs/log_local_sftp.log | grep SFTP
```

### Empty Output Directory

```
⚠️ Fichier introuvable: output/helios/siicea_*.csv
```

**Solution:**

```bash
# Check if views were created
duckdb data/helios/duckdb_database.duckdb -c "SHOW TABLES;"

# Check DBT logs
cat logs/log_local_sftp.log | grep -A 5 "DBT"

# Verify output directory exists
mkdir -p output/helios
```

### DBT Errors

```bash
# Check DBT compilation
cat dbtHelios/target/run_results.json

# Review logs
tail -100 logs/log_local_sftp.log

# Test DBT manually
cd dbtHelios
dbt run --profiles-dir .. --profile Helios --target local
```

## SFTP Setup

### Quick Setup

```bash
# 1. Create .env file
cat > .env << 'EOF'
SFTP_HOST="your.sftp.host"
SFTP_PORT=22
SFTP_USERNAME="your_username"
SFTP_PRIVATE_KEY_PATH="~/.ssh/id_rsa"
EOF

# 2. Set permissions
chmod 600 .env
chmod 600 ~/.ssh/id_rsa

# 3. Test connection
ssh -i ~/.ssh/id_rsa username@sftp.host
```

### Detailed Setup

See: `../anais_staging/PRIVATE_KEY_SETUP_GUIDE.md`

## Commands Reference

```bash
# Local export only
uv run run_local_with_sftp.py --env "local" --profile "Helios"

# With SFTP upload
uv run run_local_with_sftp.py --env "local" --profile "Helios" --use-sftp

# Query database
duckdb data/helios/duckdb_database.duckdb

# Check logs
cat logs/log_local_sftp.log

# List output files
ls -lh output/helios/
```

## Configuration

### metadata.yml

Key configuration settings:

```yaml
Helios:
  local_directory_output: "output/helios/"
  remote_directory_output: "/SCN_BDD/HELIOS/output/"

  # Tables copied from Staging
  table_to_copy:
    staging__sa_sivss: sa_sivss
    staging__sa_sirec: sa_sirec
    staging__sa_siicea_decisions: sa_siicea_decisions
    staging__sa_siicea_missions_real: sa_siicea_missions_real

  # Views to export/upload (base names, dates are added dynamically)
  files_to_upload:
    helios__missions: siicea      # Output: siicea_YYYYMMDD.csv
    helios__sirec: sirec          # Output: sirec_YYYYMMDD.csv
    helios__sivss: sivss          # Output: sivss_YYYYMMDD.csv
```

**Note:** Output filenames use dates extracted from input files (via `input_files_date` table in Staging database), not today's date.

### profiles.yml

Database connections:

```yaml
Helios:
  outputs:
    local:
      type: duckdb
      path: data/helios/duckdb_database.duckdb
      schema: main
```

## What's Different from Staging?

| Feature | Staging | Helios |
|---------|---------|--------|
| **Input** | CSV files | Staging database tables |
| **SFTP** | Downloads files | Uploads files |
| **Output** | Staging views | Analytical views |
| **Direction** | SFTP → Local | Local → SFTP |
| **Purpose** | Data ingestion | Data export |

## Next Steps

After Helios pipeline completes:

1. **Verify exports:** Check `output/helios/` for CSV files
2. **Query views:** Use DuckDB to analyze data
3. **Check SFTP:** Verify files uploaded (if using `--use-sftp`)
4. **Analyze data:** Use exported CSV files for analysis/reporting

## Documentation

- **SFTP Setup:** `../anais_staging/PRIVATE_KEY_SETUP_GUIDE.md`
- **Project Overview:** `../../README.md`
- **Original README:** `README.md`

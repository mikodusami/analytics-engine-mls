# MLS Salary Analytics Engine

A Python ETL pipeline and analytics engine for Major League Soccer player salary data. Automatically scrapes, transforms, and analyzes salary information from the MLS Players Association dating back to 2007.

## Features

- **Automated Data Collection** - Scrapes salary data from MLSPA (PDF and CSV formats)
- **Multi-Format Storage** - Outputs to CSV, Parquet, and SQLite
- **Built-in Analytics** - Salary trends, team spending, top earners, distribution analysis
- **Data Quality Reports** - Identifies missing values, outliers, and duplicates
- **18 Years of Data** - 11,600+ player salary records from 2007-2025

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/mls-salary-analytics.git
cd mls-salary-analytics

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

```bash
# Run full ETL pipeline (scrape, transform, load)
python main.py run

# View available data sources
python main.py discover

# Run data quality report
python main.py quality

# Analyze salary trends
python main.py analyze --type trends
```

## CLI Commands

### ETL Pipeline

```bash
python main.py run                      # Full pipeline, all formats
python main.py run --format csv         # CSV only
python main.py run --format parquet     # Parquet only
python main.py run --format sqlite      # SQLite only
python main.py run --year 2025          # Single year
python main.py run --split              # Separate CSV per year
```

### Analytics

```bash
python main.py analyze --type trends        # Salary trends over time
python main.py analyze --type teams         # Team spending comparison
python main.py analyze --type teams --year 2025  # Team spending for specific year
python main.py analyze --type top           # Top 10 earners
python main.py analyze --type top --top 20  # Top 20 earners
python main.py analyze --type distribution  # Salary distribution stats
```

### Data Quality

```bash
python main.py quality    # Full data quality report
```

## Output Files

After running the ETL pipeline:

| File                      | Format  | Description                |
| ------------------------- | ------- | -------------------------- |
| `output/salaries.csv`     | CSV     | All records in single file |
| `output/salaries.parquet` | Parquet | Optimized for analytics    |
| `output/salaries.db`      | SQLite  | Queryable database         |

## Project Structure

```
├── main.py                 # CLI entry point
├── ingestion/              # Data extraction layer
│   ├── scrapers.py         # Base scraper class
│   ├── salary_scraper.py   # MLSPA salary scraper
│   ├── parsers.py          # Base parser class
│   ├── pdf_salary_parser.py
│   └── csv_salary_parser.py
├── transform/              # Data transformation layer
│   ├── salary_transformer.py
│   ├── header_detector.py
│   ├── clubs.py            # Club name normalization
│   ├── cleaners.py         # Value cleaning utilities
│   └── schema.py           # SalaryRecord dataclass
├── load/                   # Data loading layer
│   └── csv_writer.py
├── storage/                # Storage backends
│   ├── database.py         # SQLite storage
│   └── parquet.py          # Parquet storage
├── analytics/              # Analytics layer
│   ├── salary_analytics.py # Analysis functions
│   └── data_quality.py     # Data validation
└── output/                 # Generated data files
```

## Data Schema

| Field             | Type   | Description                   |
| ----------------- | ------ | ----------------------------- |
| `year`            | int    | Salary year                   |
| `club`            | string | Team name (normalized)        |
| `last_name`       | string | Player last name              |
| `first_name`      | string | Player first name             |
| `position`        | string | Playing position              |
| `base_salary`     | float  | Base salary (USD)             |
| `guaranteed_comp` | float  | Guaranteed compensation (USD) |

## Sample Analytics Output

### Top Earners 2025

```
 club               first_name  last_name  position           guaranteed_comp
 Inter Miami        Lionel      Messi      RIGHT WING         $20,446,668
 LAFC               Heung-min   Son        LEFT WING          $11,152,852
 Inter Miami        Sergio      Busquets   DEFENSIVE MIDFIELD  $8,774,996
 Atlanta United     Miguel      Almirón    RIGHT WING          $7,871,000
 San Diego FC       Hirving     Lozano     LEFT WING           $7,633,333
```

### Salary Growth Over Time

```
Year    Avg Salary    YoY Growth
2007    $102,096      -
2010    $153,404      +29.2%
2015    $257,809      +28.1%
2020    $366,198      +10.4%
2025    $555,033      +8.2%
```

## Using the Data Programmatically

### Python with Pandas

```python
import pandas as pd

# Load from Parquet (fastest)
df = pd.read_parquet("output/salaries.parquet")

# Top earners by year
top_by_year = df.groupby("year").apply(lambda x: x.nlargest(1, "guaranteed_comp"))

# Team spending trends
team_spending = df.groupby(["year", "club"])["guaranteed_comp"].sum().unstack()
```

### SQLite Queries

```python
import sqlite3

conn = sqlite3.connect("output/salaries.db")

# Average salary by position
query = """
    SELECT position, AVG(guaranteed_comp) as avg_salary
    FROM salaries
    WHERE year = 2025 AND position != ''
    GROUP BY position
    ORDER BY avg_salary DESC
"""
results = conn.execute(query).fetchall()
```

## Data Source

Salary data is sourced from the [MLS Players Association Salary Guide](https://mlsplayers.org/resources/salary-guide).

## License

MIT

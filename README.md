# MLS Salary Analytics Engine

A comprehensive Python ETL pipeline and analytics platform for Major League Soccer data. Automatically scrapes, transforms, and analyzes player salary information, team rosters, and performance statistics.

## Features

- **Salary Data Pipeline** — Scrapes salary data from MLSPA (PDF and CSV formats) dating back to 2007
- **Roster Scraping** — Extracts current team rosters and player profiles from mlssoccer.com
- **Player Statistics** — Collects detailed player stats by season (general, passing, attacking, defending, goalkeeping)
- **Multi-Format Storage** — Outputs to CSV, Parquet, and SQLite
- **Built-in Analytics** — Salary trends, team spending, top earners, distribution analysis
- **Data Quality Reports** — Identifies missing values, outliers, and duplicates

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

# Install Playwright browsers (required for roster/stats scraping)
playwright install
```

## Quick Start

```bash
# Run salary ETL pipeline
python main.py run

# Scrape team rosters
python main.py roster

# Scrape player statistics
python main.py stats

# Run analytics
python main.py analyze --type trends
```

## CLI Reference

### Salary Pipeline

```bash
python main.py discover                 # List available salary sources
python main.py run                      # Full pipeline, all formats
python main.py run --format csv         # CSV output only
python main.py run --format parquet     # Parquet output only
python main.py run --format sqlite      # SQLite output only
python main.py run --year 2025          # Process single year
python main.py run --split              # Separate CSV per year
python main.py run --output ./data      # Custom output directory
```

### Roster Scraping

```bash
python main.py roster                           # Scrape all team rosters
python main.py roster --team inter-miami-cf     # Scrape specific team
python main.py roster --format csv              # CSV output only
python main.py roster --format parquet          # Parquet output only
python main.py roster --no-headless             # Show browser window
```

### Player Statistics

```bash
python main.py stats                            # Scrape all teams, all seasons
python main.py stats --team atlanta-united      # Scrape specific team
python main.py stats --season 2024              # Scrape specific season
python main.py stats --season 2023 --season 2024  # Multiple seasons
python main.py stats --no-profiles              # Skip player profile details (faster)
python main.py stats --no-headless              # Show browser window
```

### Analytics

```bash
python main.py analyze --type trends            # Salary trends over time
python main.py analyze --type teams             # Team spending comparison
python main.py analyze --type teams --year 2025 # Team spending for specific year
python main.py analyze --type top               # Top 10 earners
python main.py analyze --type top --top 20      # Top N earners
python main.py analyze --type distribution      # Salary distribution stats
```

### Data Quality

```bash
python main.py quality                  # Full data quality report
```

## Output Files

| File                              | Format  | Description                         |
| --------------------------------- | ------- | ----------------------------------- |
| `output/salaries.csv`             | CSV     | All salary records                  |
| `output/salaries.parquet`         | Parquet | Salary data optimized for analytics |
| `output/salaries.db`              | SQLite  | Queryable salary database           |
| `output/mls_rosters.csv`          | CSV     | Team rosters with player details    |
| `output/mls_rosters.parquet`      | Parquet | Roster data for analytics           |
| `output/mls_teams.csv`            | CSV     | Team metadata                       |
| `output/mls_player_stats.csv`     | CSV     | Player statistics by season         |
| `output/mls_player_stats.parquet` | Parquet | Stats data for analytics            |

## Project Structure

```
├── main.py                     # CLI entry point
├── ingestion/                  # Data extraction layer
│   ├── scrapers.py             # Base scraper class
│   ├── salary_scraper.py       # MLSPA salary scraper
│   ├── mls_roster_scraper.py   # Team roster scraper
│   ├── mls_stats_scraper.py    # Player stats scraper
│   ├── playwright_scraper.py   # Browser automation base
│   ├── parsers.py              # Base parser class
│   ├── pdf_salary_parser.py    # PDF parsing
│   └── csv_salary_parser.py    # CSV parsing
├── transform/                  # Data transformation layer
│   ├── salary_transformer.py   # Salary data normalization
│   ├── mls_roster_transformer.py
│   ├── mls_stats_transformer.py
│   ├── schema.py               # SalaryRecord dataclass
│   ├── mls_schema.py           # MLSPlayer/MLSTeam dataclasses
│   ├── mls_stats_schema.py     # MLSPlayerStats dataclass
│   ├── header_detector.py      # Column detection
│   ├── clubs.py                # Club name normalization
│   └── cleaners.py             # Value cleaning utilities
├── load/                       # Data loading layer
│   ├── csv_writer.py           # CSV output
│   ├── mls_writer.py           # Roster output
│   └── mls_stats_writer.py     # Stats output
├── storage/                    # Storage backends
│   ├── database.py             # SQLite storage
│   └── parquet.py              # Parquet storage
├── analytics/                  # Analytics layer
│   ├── salary_analytics.py     # Analysis functions
│   └── data_quality.py         # Data validation
└── output/                     # Generated data files
```

## Data Schemas

### Salary Records

| Field             | Type   | Description                   |
| ----------------- | ------ | ----------------------------- |
| `year`            | int    | Salary year                   |
| `club`            | string | Team name (normalized)        |
| `last_name`       | string | Player last name              |
| `first_name`      | string | Player first name             |
| `position`        | string | Playing position              |
| `base_salary`     | float  | Base salary (USD)             |
| `guaranteed_comp` | float  | Guaranteed compensation (USD) |

### Roster Records

| Field             | Type    | Description                  |
| ----------------- | ------- | ---------------------------- |
| `team_name`       | string  | Team name                    |
| `team_slug`       | string  | URL-friendly team identifier |
| `player_name`     | string  | Player name                  |
| `player_url`      | string  | Profile URL                  |
| `jersey_number`   | string  | Jersey number                |
| `position`        | string  | Playing position             |
| `roster_category` | string  | Roster designation           |
| `player_category` | string  | Player category              |
| `player_status`   | string  | Current status               |
| `profile_*`       | various | Additional profile details   |

### Stats Records

| Field         | Type    | Description                                                    |
| ------------- | ------- | -------------------------------------------------------------- |
| `team_name`   | string  | Team name                                                      |
| `season`      | int     | Season year                                                    |
| `stat_type`   | string  | Category (general, passing, attacking, defending, goalkeeping) |
| `player_name` | string  | Player name                                                    |
| `club`        | string  | Club abbreviation                                              |
| `stat_*`      | various | Stat-specific columns                                          |
| `profile_*`   | various | Player profile details                                         |

## Usage Examples

### Python with Pandas

```python
import pandas as pd

# Load salary data
df = pd.read_parquet("output/salaries.parquet")

# Top earners by year
top_by_year = df.groupby("year").apply(lambda x: x.nlargest(1, "guaranteed_comp"))

# Team spending trends
team_spending = df.groupby(["year", "club"])["guaranteed_comp"].sum().unstack()

# Load roster data
rosters = pd.read_parquet("output/mls_rosters.parquet")

# Players by position
by_position = rosters.groupby("position").size()
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

## Requirements

- Python 3.8+
- playwright
- requests
- beautifulsoup4
- pypdf
- pandas
- pyarrow

## Data Sources

- Salary data: [MLS Players Association Salary Guide](https://mlsplayers.org/resources/salary-guide)
- Roster and stats: [mlssoccer.com](https://www.mlssoccer.com)

## License

MIT

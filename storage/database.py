"""
SQLite database storage for salary data.
"""
import sqlite3
import logging
from pathlib import Path
from typing import List, Optional
from transform.schema import SalaryRecord

logger = logging.getLogger(__name__)


class SalaryDatabase:
    """SQLite database for MLS salary data."""
    
    def __init__(self, db_path: str = "output/salaries.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Create tables if they don't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS salaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    club TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    first_name TEXT,
                    position TEXT,
                    base_salary REAL NOT NULL,
                    guaranteed_comp REAL NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_year ON salaries(year)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_club ON salaries(club)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_position ON salaries(position)")
            conn.commit()
        logger.info(f"Database initialized at {self.db_path}")
    
    def insert_records(self, records: List[SalaryRecord], clear_existing: bool = False):
        """Insert salary records into database."""
        with sqlite3.connect(self.db_path) as conn:
            if clear_existing:
                conn.execute("DELETE FROM salaries")
            
            conn.executemany("""
                INSERT INTO salaries (year, club, last_name, first_name, position, base_salary, guaranteed_comp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [(r.year, r.club, r.last_name, r.first_name, r.position, r.base_salary, r.guaranteed_comp) 
                  for r in records])
            conn.commit()
        
        logger.info(f"Inserted {len(records)} records into database")

    def query(self, sql: str, params: tuple = ()) -> List[tuple]:
        """Execute a query and return results."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(sql, params)
            return cursor.fetchall()
    
    def get_years(self) -> List[int]:
        """Get all years in database."""
        rows = self.query("SELECT DISTINCT year FROM salaries ORDER BY year")
        return [r[0] for r in rows]
    
    def get_clubs(self, year: Optional[int] = None) -> List[str]:
        """Get all clubs, optionally filtered by year."""
        if year:
            rows = self.query("SELECT DISTINCT club FROM salaries WHERE year = ? ORDER BY club", (year,))
        else:
            rows = self.query("SELECT DISTINCT club FROM salaries ORDER BY club")
        return [r[0] for r in rows]
    
    def count_records(self, year: Optional[int] = None) -> int:
        """Count records, optionally filtered by year."""
        if year:
            rows = self.query("SELECT COUNT(*) FROM salaries WHERE year = ?", (year,))
        else:
            rows = self.query("SELECT COUNT(*) FROM salaries")
        return rows[0][0]

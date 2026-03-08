"""
SQLite database storage for salary data.

SQLite is a file-based database - no server needed. Perfect for local
analytics and querying. The data lives in a single .db file that you
can copy around, back up, or query with any SQLite tool.

This is the "L" in ETL for folks who want SQL access to the data.
"""
import sqlite3
import logging
from pathlib import Path
from typing import List, Optional
from transform.schema import SalaryRecord

logger = logging.getLogger(__name__)


class SalaryDatabase:
    """
    SQLite database for MLS salary data.
    
    Creates a salaries table with indexes for common query patterns.
    Supports insert, query, and basic aggregation operations.
    
    Usage:
        db = SalaryDatabase("output/salaries.db")
        db.insert_records(records, clear_existing=True)
        years = db.get_years()
    """
    
    def __init__(self, db_path: str = "output/salaries.db"):
        """
        Initialize database connection.
        
        Creates the database file and tables if they don't exist.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        # Create parent directories if needed
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """
        Create tables and indexes if they don't exist.
        
        Schema:
        - id: Auto-incrementing primary key
        - year, club, last_name, first_name, position: The usual suspects
        - base_salary, guaranteed_comp: Raw salary strings (not cleaned)
        - created_at: When the record was inserted
        """
        with sqlite3.connect(self.db_path) as conn:
            # Create the main salaries table
            # Salaries are TEXT because we keep them as raw strings
            conn.execute("""
                CREATE TABLE IF NOT EXISTS salaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER NOT NULL,
                    club TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    first_name TEXT,
                    position TEXT,
                    base_salary TEXT NOT NULL,
                    guaranteed_comp TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Create indexes for common query patterns
            # These make WHERE clauses on these columns much faster
            conn.execute("CREATE INDEX IF NOT EXISTS idx_year ON salaries(year)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_club ON salaries(club)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_position ON salaries(position)")
            conn.commit()
        logger.info(f"Database initialized at {self.db_path}")
    
    def insert_records(self, records: List[SalaryRecord], clear_existing: bool = False):
        """
        Insert salary records into database.
        
        Args:
            records: List of SalaryRecord objects to insert
            clear_existing: If True, delete all existing records first
        """
        with sqlite3.connect(self.db_path) as conn:
            if clear_existing:
                conn.execute("DELETE FROM salaries")
            
            # Use executemany for bulk insert (much faster than individual inserts)
            conn.executemany("""
                INSERT INTO salaries (year, club, last_name, first_name, position, base_salary, guaranteed_comp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [(r.year, r.club, r.last_name, r.first_name, r.position, r.base_salary, r.guaranteed_comp) 
                  for r in records])
            conn.commit()
        
        logger.info(f"Inserted {len(records)} records into database")

    def query(self, sql: str, params: tuple = ()) -> List[tuple]:
        """
        Execute a raw SQL query and return results.
        
        For when you need to run custom queries.
        
        Args:
            sql: SQL query string
            params: Query parameters (for parameterized queries)
            
        Returns:
            List of result tuples
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(sql, params)
            return cursor.fetchall()
    
    def get_years(self) -> List[int]:
        """Get all years in database."""
        rows = self.query("SELECT DISTINCT year FROM salaries ORDER BY year")
        return [r[0] for r in rows]
    
    def get_clubs(self, year: Optional[int] = None) -> List[str]:
        """
        Get all clubs, optionally filtered by year.
        
        Args:
            year: Filter to specific year, or None for all years
        """
        if year:
            rows = self.query("SELECT DISTINCT club FROM salaries WHERE year = ? ORDER BY club", (year,))
        else:
            rows = self.query("SELECT DISTINCT club FROM salaries ORDER BY club")
        return [r[0] for r in rows]
    
    def count_records(self, year: Optional[int] = None) -> int:
        """
        Count records, optionally filtered by year.
        
        Args:
            year: Filter to specific year, or None for all years
        """
        if year:
            rows = self.query("SELECT COUNT(*) FROM salaries WHERE year = ?", (year,))
        else:
            rows = self.query("SELECT COUNT(*) FROM salaries")
        return rows[0][0]

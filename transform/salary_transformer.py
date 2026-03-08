"""
Main transformer for salary data.

This is the beast that takes raw rows from the parser and produces
clean SalaryRecord objects. It handles both CSV (easy) and PDF (nightmare)
formats with different parsing strategies.

The transformer has to deal with:
- Different column orders across years
- Club names appearing before or after player names
- Position codes in random places
- Salary values with various formatting
- PDF text that's been split into random tokens

pain.
"""
import logging
from typing import Optional
from transform.schema import SalaryRecord
from transform.header_detector import find_header_row, detect_column_order
from transform.clubs import normalize_club, CLUB_ALIASES
from transform.cleaners import clean_salary, clean_name, clean_position

logger = logging.getLogger(__name__)


class SalaryTransformer:
    """
    Transforms raw salary rows into normalized SalaryRecord objects.
    
    Two parsing modes:
    1. Index mode (CSV): Direct column access using header mapping
    2. Heuristic mode (PDF): Pattern matching to find values in token soup
    
    Usage:
        transformer = SalaryTransformer(year=2024, source_format="csv")
        records = transformer.transform(rows)
    """
    
    def __init__(self, year: int, source_format: str = "pdf"):
        """
        Initialize transformer for a specific year.
        
        Args:
            year: The year of the data (used in output records)
            source_format: "pdf" or "csv" - determines parsing strategy
        """
        self.year = year
        self.source_format = source_format
        self.column_map: dict[str, int] = {}  # Maps field name to column index
        self.header_row_idx: int = -1
        self.club_first: bool = True  # Whether club appears before names in the data
    
    def transform(self, rows: list[list[str]]) -> list[SalaryRecord]:
        """
        Transform raw rows into SalaryRecords.
        
        Steps:
        1. Find the header row
        2. Detect column order from header
        3. Parse each data row into a SalaryRecord
        
        Args:
            rows: Raw rows from the parser
            
        Returns:
            List of SalaryRecord objects
        """
        # Step 1: Find header row
        self.header_row_idx = find_header_row(rows)
        if self.header_row_idx == -1:
            logger.warning(f"No header found for year {self.year}")
            return []
        
        # Step 2: Detect column order
        header = rows[self.header_row_idx]
        self.column_map = detect_column_order(header)
        
        # Determine if club comes before or after names
        club_idx = self.column_map.get("club", 0)
        name_idx = self.column_map.get("last_name", self.column_map.get("first_name", 1))
        self.club_first = club_idx < name_idx
        
        logger.info(f"Year {self.year}: header at row {self.header_row_idx}, "
                   f"club_first={self.club_first}, columns: {self.column_map}")
        
        # Step 3: Process data rows (everything after header)
        records = []
        data_rows = rows[self.header_row_idx + 1:]
        
        for row in data_rows:
            if not row or self._is_empty_row(row):
                continue
            
            record = self._parse_row(row)
            if record:
                records.append(record)
        
        logger.info(f"Year {self.year}: transformed {len(records)} records")
        return records
    
    def _is_empty_row(self, row: list[str]) -> bool:
        """Check if row is effectively empty (all whitespace)."""
        return all(not cell.strip() for cell in row)
    
    def _parse_row(self, row: list[str]) -> Optional[SalaryRecord]:
        """
        Parse a single data row into a SalaryRecord.
        
        Routes to index-based or heuristic parsing based on format.
        """
        try:
            # CSV files with complete headers use direct index access
            if self._has_complete_mapping():
                return self._parse_row_by_index(row)
            
            # PDF files use heuristic parsing (pattern matching)
            return self._parse_row_heuristic(row)
            
        except Exception as e:
            logger.debug(f"Failed to parse row {row}: {e}")
            return None
    
    def _has_complete_mapping(self) -> bool:
        """
        Check if we have enough column mappings for direct index access.
        
        Only use index mode for CSV files with all required columns mapped.
        """
        if self.source_format != "csv":
            return False
        
        required = {"club", "first_name", "last_name", "base_salary", "guaranteed_comp"}
        return required.issubset(self.column_map.keys())
    
    def _parse_row_by_index(self, row: list[str]) -> Optional[SalaryRecord]:
        """
        Parse row using direct column index access (for CSV).
        
        This is the easy path - just grab values by column index.
        """
        try:
            club = row[self.column_map["club"]].strip()
            first_name = clean_name(row[self.column_map["first_name"]])
            last_name = clean_name(row[self.column_map["last_name"]])
            base_salary = clean_salary(row[self.column_map["base_salary"]])
            guaranteed_comp = clean_salary(row[self.column_map["guaranteed_comp"]])
            
            # Position is optional
            position = ""
            if "position" in self.column_map:
                position = clean_position(row[self.column_map["position"]])
            
            # Validate salaries
            if base_salary is None or guaranteed_comp is None:
                return None
            
            return SalaryRecord(
                year=self.year,
                club=club,
                last_name=last_name,
                first_name=first_name,
                position=position,
                base_salary=base_salary,
                guaranteed_comp=guaranteed_comp,
            )
        except (IndexError, KeyError) as e:
            logger.debug(f"Index access failed for row {row}: {e}")
            return None
    
    def _parse_row_heuristic(self, row: list[str]) -> Optional[SalaryRecord]:
        """
        Parse row using heuristics (for PDF with split tokens).
        
        This is the hard path. PDF text extraction produces a list of tokens
        that we have to reassemble into meaningful fields. We use salary
        values as anchors since they're easy to identify ($ or numbers).
        """
        try:
            # Find salary values first - they anchor our parsing
            salary_indices = self._find_salary_indices(row)
            if len(salary_indices) < 2:
                return None  # Need at least base and guaranteed
            
            # Last two salary-like values are base and guaranteed
            base_idx = salary_indices[-2]
            guar_idx = salary_indices[-1]
            
            base_salary = clean_salary(row[base_idx])
            guaranteed_comp = clean_salary(row[guar_idx])
            
            if base_salary is None or guaranteed_comp is None:
                return None

            # Find position - check both before and after salaries
            position = ""
            pos_idx = self.column_map.get("position", -1)
            position_before_salary = False
            
            # If position column is after salary columns in header, check end of row
            if pos_idx != -1 and pos_idx > self.column_map.get("guaranteed_comp", guar_idx):
                pos_candidate = row[-1]  # Position at end
                if self._looks_like_position(pos_candidate):
                    position = clean_position(pos_candidate)
            # Otherwise check before salaries (traditional layout)
            elif base_idx > 0:
                pos_candidate = row[base_idx - 1]
                if self._looks_like_position(pos_candidate):
                    position = clean_position(pos_candidate)
                    position_before_salary = True
            
            # Everything before position/salaries is club + names
            end_idx = (base_idx - 1) if position_before_salary else base_idx
            prefix_tokens = row[:end_idx]
            
            # Parse club and names based on order
            if self.club_first:
                club, names = self._parse_club_first(prefix_tokens)
            else:
                club, names = self._parse_names_first(prefix_tokens)
            
            # Split names into first/last
            first_name, last_name = self._split_names(names)
            
            return SalaryRecord(
                year=self.year,
                club=club,
                last_name=last_name,
                first_name=first_name,
                position=position,
                base_salary=base_salary,
                guaranteed_comp=guaranteed_comp,
            )
            
        except Exception as e:
            logger.debug(f"Failed to parse row {row}: {e}")
            return None
    
    def _find_salary_indices(self, row: list[str]) -> list[int]:
        """
        Find indices of salary values in row.
        
        Looks for tokens with $ or bare numbers that look like salaries.
        """
        indices = []
        for i, token in enumerate(row):
            if '$' in token:
                indices.append(i)
            elif token.replace(',', '').replace('.', '').isdigit():
                # Bare number that looks like salary (at least 4 digits)
                cleaned = token.replace(',', '').replace('.', '')
                if len(cleaned) >= 4:
                    indices.append(i)
        return indices
    
    def _looks_like_position(self, token: str) -> bool:
        """
        Check if token looks like a position code.
        
        Position codes are short (1-4 chars) and alphabetic.
        Examples: "M", "F", "GK", "D", "M-F"
        """
        if not token:
            return False
        cleaned = token.replace('-', '').replace('/', '')
        return len(cleaned) <= 4 and cleaned.isalpha()
    
    def _parse_club_first(self, tokens: list[str]) -> tuple[str, list[str]]:
        """Parse tokens assuming club comes first, then names."""
        club, consumed = normalize_club(tokens)
        names = tokens[consumed:]
        return club, names
    
    def _parse_names_first(self, tokens: list[str]) -> tuple[str, list[str]]:
        """Parse tokens assuming names come first, then club."""
        # Find where club starts by looking for known club patterns
        club_start = self._find_club_start(tokens)
        
        if club_start > 0:
            names = tokens[:club_start]
            club, _ = normalize_club(tokens[club_start:])
        else:
            # Fallback: assume first 2 tokens are names
            names = tokens[:2]
            club, _ = normalize_club(tokens[2:])
        
        return club, names
    
    def _find_club_start(self, tokens: list[str]) -> int:
        """
        Find index where club name starts in token list.
        
        Looks for known abbreviations or club name keywords.
        """
        for i, token in enumerate(tokens):
            upper = token.upper()
            # Check for abbreviations
            if upper in CLUB_ALIASES:
                return i
            # Check for known club name starts
            lower = token.lower()
            if lower in ("atlanta", "austin", "chicago", "colorado", "columbus", 
                        "dc", "fc", "houston", "inter", "la", "los", "minnesota",
                        "nashville", "new", "orlando", "philadelphia", "portland",
                        "real", "san", "seattle", "sporting", "toronto", "vancouver"):
                return i
        return -1

    def _split_names(self, name_tokens: list[str]) -> tuple[str, str]:
        """
        Split name tokens into (first_name, last_name).
        
        Uses header column order to determine which comes first.
        """
        if not name_tokens:
            return "", ""
        
        if len(name_tokens) == 1:
            return "", clean_name(name_tokens[0])
        
        # Check column order from header
        last_idx = self.column_map.get("last_name", -1)
        first_idx = self.column_map.get("first_name", -1)
        
        if last_idx != -1 and first_idx != -1:
            # Use header order
            if last_idx < first_idx:
                # Last name comes first
                last_name = clean_name(name_tokens[0])
                first_name = clean_name(" ".join(name_tokens[1:]))
            else:
                # First name comes first
                first_name = clean_name(name_tokens[0])
                last_name = clean_name(" ".join(name_tokens[1:]))
        else:
            # Default: assume last_name first (most common in older data)
            last_name = clean_name(name_tokens[0])
            first_name = clean_name(" ".join(name_tokens[1:]))
        
        return first_name, last_name
    
    def _looks_like_position(self, token: str) -> bool:
        """Check if token looks like a position code."""
        if not token:
            return False
        cleaned = token.replace('-', '').replace('/', '')
        return len(cleaned) <= 4 and cleaned.isalpha()
    
    def _parse_club_first(self, tokens: list[str]) -> tuple[str, list[str]]:
        """Parse tokens assuming club comes first."""
        club, consumed = normalize_club(tokens)
        names = tokens[consumed:]
        return club, names
    
    def _parse_names_first(self, tokens: list[str]) -> tuple[str, list[str]]:
        """Parse tokens assuming names come first, then club."""
        # Find where club starts by looking for known club patterns
        club_start = self._find_club_start(tokens)
        
        if club_start > 0:
            names = tokens[:club_start]
            club, _ = normalize_club(tokens[club_start:])
        else:
            # Fallback: assume first 2 tokens are names
            names = tokens[:2]
            club, _ = normalize_club(tokens[2:])
        
        return club, names
    
    def _find_club_start(self, tokens: list[str]) -> int:
        """Find index where club name starts in token list."""
        for i, token in enumerate(tokens):
            upper = token.upper()
            # Check for abbreviations
            if upper in CLUB_ALIASES:
                return i
            # Check for known club name starts
            lower = token.lower()
            if lower in ("atlanta", "austin", "chicago", "colorado", "columbus", 
                        "dc", "fc", "houston", "inter", "la", "los", "minnesota",
                        "nashville", "new", "orlando", "philadelphia", "portland",
                        "real", "san", "seattle", "sporting", "toronto", "vancouver"):
                return i
        return -1

    def _split_names(self, name_tokens: list[str]) -> tuple[str, str]:
        """Split name tokens into (first_name, last_name)."""
        if not name_tokens:
            return "", ""
        
        if len(name_tokens) == 1:
            return "", clean_name(name_tokens[0])
        
        # Check column order from header
        last_idx = self.column_map.get("last_name", -1)
        first_idx = self.column_map.get("first_name", -1)
        
        if last_idx != -1 and first_idx != -1:
            # Use header order
            if last_idx < first_idx:
                # Last name comes first
                last_name = clean_name(name_tokens[0])
                first_name = clean_name(" ".join(name_tokens[1:]))
            else:
                # First name comes first
                first_name = clean_name(name_tokens[0])
                last_name = clean_name(" ".join(name_tokens[1:]))
        else:
            # Default: assume last_name first (most common in older data)
            last_name = clean_name(name_tokens[0])
            first_name = clean_name(" ".join(name_tokens[1:]))
        
        return first_name, last_name

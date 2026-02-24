"""
Club name normalization.
Maps various club name formats to canonical names.
"""

# Map abbreviations and variations to canonical names
CLUB_ALIASES = {
    # Abbreviations (older PDFs)
    "ATL": "Atlanta United",
    "ATX": "Austin FC",
    "CHI": "Chicago Fire",
    "CHV": "Chivas USA",
    "CIN": "FC Cincinnati",
    "CLB": "Columbus Crew",
    "COL": "Colorado Rapids",
    "DAL": "FC Dallas",
    "DC": "DC United",
    "HOU": "Houston Dynamo",
    "KC": "Sporting Kansas City",
    "LA": "LA Galaxy",
    "LAFC": "Los Angeles FC",
    "MIA": "Inter Miami",
    "MIN": "Minnesota United",
    "MTL": "CF Montreal",
    "NE": "New England Revolution",
    "NSH": "Nashville SC",
    "NY": "New York Red Bulls",
    "NYC": "New York City FC",
    "ORL": "Orlando City",
    "PHI": "Philadelphia Union",
    "POR": "Portland Timbers",
    "RSL": "Real Salt Lake",
    "SEA": "Seattle Sounders",
    "SJ": "San Jose Earthquakes",
    "SKC": "Sporting Kansas City",
    "TFC": "Toronto FC",
    "VAN": "Vancouver Whitecaps",
}

# Multi-word club names for reconstruction
KNOWN_CLUBS = {
    "atlanta united",
    "austin fc", 
    "cf montreal",
    "chicago fire",
    "chivas usa",
    "colorado rapids",
    "columbus crew",
    "dc united",
    "fc cincinnati",
    "fc dallas",
    "houston dynamo",
    "inter miami",
    "la galaxy",
    "los angeles fc",
    "minnesota united",
    "nashville sc",
    "new england revolution",
    "new york city fc",
    "new york red bulls",
    "orlando city",
    "philadelphia union",
    "portland timbers",
    "real salt lake",
    "san jose earthquakes",
    "seattle sounders",
    "sporting kansas city",
    "toronto fc",
    "vancouver whitecaps",
}


def normalize_club(tokens: list[str]) -> tuple[str, int]:
    """
    Given a list of tokens starting with club name, extract and normalize it.
    Returns (normalized_name, tokens_consumed).
    """
    if not tokens:
        return "", 0
    
    # Check if first token is an abbreviation
    first = tokens[0].upper()
    if first in CLUB_ALIASES:
        return CLUB_ALIASES[first], 1
    
    # Try to match multi-word club names
    for length in range(min(4, len(tokens)), 0, -1):
        candidate = " ".join(tokens[:length]).lower()
        if candidate in KNOWN_CLUBS:
            # Return with proper capitalization
            return candidate.title(), length
    
    # Fallback: return first token as-is
    return tokens[0], 1

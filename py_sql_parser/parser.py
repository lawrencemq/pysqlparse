import re
from typing import Set

table_names_regex = re.compile(r'(?<=from|join)\s+[\w\.\-\"]+', re.IGNORECASE)
aliases_from_with_regex = re.compile(r'[\w\.\-]+(?=\s+as\s*\()', re.IGNORECASE)


def _standardize_query(query: str) -> str:
    """
    Removes string literals like \r\n or \n and replaces them with whitespace.
    """
    return query.replace("\r", " ").replace("\\r", " ").replace("\\n", " ").strip()


def _remove_comments(query: str) -> str:
    """
    This should remove comments from SQL
    Removes anything between '--' and '\n'
    Removes /* ... */
    Includes cases with more than one commented line
    """
    return re.sub(re.compile(r"/\*.*?\*/", re.DOTALL), "",  # Handles /* Comment */
                  re.sub(r'--.*?\n', '', query, flags=re.DOTALL)  # Handles -- Comment
                  ).strip()


def get_table_names(query: str) -> Set[str]:
    clean_query = _remove_comments(_standardize_query(query))
    table_aliases = set(name.lower() for name in aliases_from_with_regex.findall(clean_query)).union({'unnest'})
    table_names = set(name.strip().lower().replace('"', '') for name in table_names_regex.findall(clean_query))
    return table_names - table_aliases

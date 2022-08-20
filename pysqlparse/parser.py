import re
from typing import Set, Tuple

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


def _find_tables_in_multi_select(query: str) -> Tuple[Set[str], Set[str]]:
    multi_select_regex = r'(?<=(from|join)).*(?=where|group|order)'
    tables_found = set()
    aliases_found = set()

    table_matches = re.search(multi_select_regex, query, re.IGNORECASE)
    if table_matches:
        table_select_string = table_matches.group(0)
        table_declarations = table_select_string.split(',')
        for table_declaration in table_declarations:
            declaration_pieces = table_declaration.strip().split(' ')
            table_name = declaration_pieces[0]
            tables_found.add(table_name)
            if len(declaration_pieces) > 1:
                aliases_found.add(declaration_pieces[-1])

    return tables_found, aliases_found


def _stdize_name(table_name: str) -> str:
    return table_name.strip().lower().replace('"', '')


def get_table_names(query: str) -> Set[str]:
    clean_query = _remove_comments(_standardize_query(query))

    table_names, table_aliases = _find_tables_in_multi_select(clean_query)
    table_aliases = table_aliases.union(set(map(_stdize_name, aliases_from_with_regex.findall(clean_query)))).union(
        {'unnest'})
    table_names = table_names.union(set(map(_stdize_name, table_names_regex.findall(clean_query))))
    return table_names - table_aliases

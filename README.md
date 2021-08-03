# PySqlParse

Find table names and other information in a SQL query

# Installation

```
pip3 install pysqlparse
```

# Use

## Import 

```python
from pysqlparse import parser
...
```


# get_table_names

Returns a set of all table names (without aliases) found in the SQL string.

```python
from pysqlparse import parser
print(parser.get_table_names('''
        SELECT *
            FROM requests.by_account m
            INNER JOIN customer_data.styles s ON m.version = s.id
            LEFT JOIN profiles.users u ON m.csm = u.id
      '''))
```
Returns:

```python
{'request.by_account', 'customer_data.styles', 'profiles.users'}
```



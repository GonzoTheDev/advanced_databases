# Base Table Query

The base table query simply returns all rows are edition year 2020. This column is then indexed to speed up the query. The CSV for this table is votes_fact.csv.


# Collection Table Query
The collection table stores all of the vote IDs belonging to each viewer by viewerid in a list, and the query simply returns all rows for this table. The CSV for this table is viewers.csv.

# Materialized View
Due to time contraints I did not complete this part of the assignment and only getting so far as setting the list to type FROZEN.
# Advanced Databases CA 1 - Overview
This repository contains 7 files, here is a description of what each file does and the order they should be executed. 

1. create_database.sql - This file creates the new dimensional database (MusicCompDB_DIM) and defines its structure/entities/relationships.
2. etl.sql - This file collects the data from the initially provided database (MusicCompDB), then converts the data to fit the new dimensional database, including a staging phase in which the votes table is denormalised and coverted to a fact table called votes_fact.
3. questions.sql - This file contains the 3 queries implemented to anwer the questions provided in the CA. It also includes the code to generate performance statistics pre and post indexing.
4. indexs.sql - This file contains the SQL to implement the revelant indexes to improve query performance.
5. drop_indexes.sql - This file contains the SQL to drop the indexes if necessary for testing.
6. export.py - This is the python pipeline that exports all of the data from the dimensional database (MusicCompDB_DIM) and formats the tables and rows to documents in a CouchDB database. This script also includes the code to implement the required views and queries.
7. couchdb_query.py - The code to implement a simple query against the fact table in CouchDB.
import mariadb
import json
import collections
import cassandra
from cassandra.cluster import Cluster
import time
import decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Connect to MariaDB
db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "mariadb",
    "database": "MusicCompDB_DIM"
}

conn = mariadb.connect(**db_config)
cursor = conn.cursor()
cursor.execute("USE MusicCompDB_DIM") 

# Retrieve the data
fact_query = """
SELECT VOTES_FACT.* FROM VOTES_FACT 
INNER JOIN COUNTY_DIM ON VOTES_FACT.COUNTYID = COUNTY_DIM.COUNTYID
INNER JOIN AGEGROUP_DIM ON VOTES_FACT.AGE_GROUPID = AGEGROUP_DIM.AGE_GROUPID
WHERE COUNTY_DIM.COUNTYNAME IN ('Wicklow', 'Kerry')
"""
cursor.execute(fact_query)
factdata = cursor.fetchall()

# Construct an array from the resultset
rowarray_list = []
for fact in factdata:
    t = (fact[0], fact[1], fact[2], fact[3], fact[4], fact[5], fact[6], fact[7], fact[8], fact[9])
    rowarray_list.append(t)

# Dump the array to a json file
j = json.dumps(rowarray_list, cls=DecimalEncoder)
with open("mariadbfacts.json", "w") as f:
    f.write(j)

# Connect to Cassandra
casscluster = Cluster(['localhost'], port=9042)
session = casscluster.connect('c20703429')

# Create a keyspace in Cassandra
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS c20703429 
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
""")

# Create a table in Cassandra
session.execute("""
    CREATE TABLE IF NOT EXISTS c20703429.votes_fact (
        vote_id int PRIMARY KEY,
        viewerid int,
        age_groupid int,
        countyid int,
        edyear int,
        partname varchar,
        vote_category int,
        votemode varchar,
        vote int,
        vote_cost decimal
    );
""")

# Insert data into Cassandra from the json file
with open("mariadbfacts.json", "r") as f:
    data = json.load(f)
    for row in data:
        session.execute(
            """
            INSERT INTO c20703429.votes_fact (vote_id, viewerid, age_groupid, countyid, edyear, partname, vote_category, votemode, vote, vote_cost)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9])
        )

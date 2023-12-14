from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Connect to Cassandra
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('c20703429')

# Prepare the statements
select_stmt = session.prepare("SELECT vote_id, viewerid FROM votes_fact")
update_stmt = session.prepare("UPDATE votes_by_viewer SET vote_ids = vote_ids + ? WHERE viewerid = ?")

# Execute the select statement
rows = session.execute(select_stmt)

# For each row in the result, update the votes_by_viewer table
for row in rows:
    session.execute(update_stmt, [[row.vote_id], row.viewerid])

# Close the connection
cluster.shutdown()

import couchdb

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  

# Define the master and replica databases
master_db_name = "c20703429_musiccompdb"
replica_db_name = "c20703429_musiccompdb_replica"  # Replace with your student number

# Create the replica database if it doesn't exist
if replica_db_name not in couch:
    couch.create(replica_db_name)

# Define the selector for replication
selector = {
    "selector": {
        "$or": [
            {"_id": {"$regex": "^age_group:"}},
            {"_id": {"$regex": "^wicklow:"}},
            {"_id": {"$regex": "^kerry:"}}
        ]
    }
}

# Create the replication document
replication_doc = {
    "_id": "replication_doc",
    "source": "http://admin:couchdb@127.0.0.1:5984/c20703429_musiccompdb",
    "target": "http://admin:couchdb@127.0.0.1:5984/c20703429_musiccompdb_replica",
    "continuous": True,
    "selector": selector
}

# Save the replication document to the _replicator database
replicator_db = couch["_replicator"]
replicator_db.save(replication_doc)

print("Replication started.")

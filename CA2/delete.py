import couchdb

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  

db_name = "c20703429_musiccompdb"  
#db_name = "c20703429_musiccompdb_replica"  

couch.delete(db_name)  
import couchdb

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  
db_name = "music_comp_db"  
db = couch[db_name]

# Mango query to get all 'votes_fact' documents with a vote greater than a threshold
threshold_vote = 5

query = {
    "selector": {
        "type": "votes_fact",
        "vote": {"$gt": threshold_vote}
    }
}

result = db.find(query)
for row in result:
    print(row)

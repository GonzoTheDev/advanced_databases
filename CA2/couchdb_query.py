import json
import mariadb
import couchdb

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  
db_name = "C20703429_MusicCompDB"  
db = couch[db_name]

# Connect to MariaDB relational database
db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "mariadb",
    "database": "MusicCompDB_DIM"
}

conn = mariadb.connect(**db_config)
cursor = conn.cursor(dictionary=True)

# Get the master database
master_db = db['C20703429_MusicCompDB']

# Port data for one of the dimensions to a document
dimension_doc = {
    'countyname': 'Dublin',
}
master_db.save(dimension_doc)

# Port data for Dublin to CouchDB
cursor.execute("SELECT * FROM votes_fact WHERE countyid = 1")
dublin_votes = cursor.fetchall()

for vote in dublin_votes:
    fact_doc = {
        "county": "Dublin",
        "votes": [vote]
    }
    master_db.save(fact_doc)

# Port data for Kerry to CouchDB
cursor.execute("SELECT * FROM votes_fact WHERE countyid = 2")
kerry_votes = cursor.fetchall()

for vote in kerry_votes:
    fact_doc = {
        "county": "Kerry",
        "votes": [vote]
    }
    master_db.save(fact_doc)


# Divide the data into different partitions

# Get Dublin data
dublin_data = master_db.view("C20703429_MusicCompDB/get_dublin_data")

# Get Kerry data
kerry_data = master_db.view("C20703429_MusicCompDB/get_kerry_data")

# Create Dublin partition
dublin_partition = db.create("dublin_partition")

# Create Kerry partition
kerry_partition = db.create("kerry_partition")

# Add Dublin data to Dublin partition
for document in dublin_data:
    dublin_partition.save(document)

# Add Kerry data to Kerry partition
for document in kerry_data:
    kerry_partition.save(document)

fact_doc = {
    'votes': [
        {
            'viewerid': 3,
            'agegroupid': 3,
            'countyid': 2,
            'edyear': 2023,
            'partname': 'The Coronas',
            'vote_category': 1,
            'votemode': 'Instagram',
            'vote': 3,
            'vote_cost': 0.25,
        },
        {
            'viewerid': 4,
            'agegroupid': 4,
            'countyid': 2,
            'edyear': 2023,
            'partname': 'Kodaline',
            'vote_category': 2,
            'votemode': 'TV',
            'vote': 2,
            'vote_cost': 1.00,
        },
    ]
}
fact_doc['partition_key'] = 'county2'
master_db.save(fact_doc)

# Export all of the documents to a JSON file
with open('C20703429_MusicCompDB.json', 'w') as json_file:
    for doc in master_db.view('_all_docs'):
        document = master_db.get(doc['id'])
        json_file.write(json.dumps(document, indent=4))
        json_file.write('\n')

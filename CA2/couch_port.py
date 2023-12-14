import mariadb
import couchdb
import json
import decimal
import requests

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  
db_name = "c20703429_musiccompdb"  

if db_name in couch:
    #couch.delete(db_name)  # Delete the database if it already exists
    #db = couch.create(db_name)
    #print(f"Database '{db_name}' created successfully.")
    # Get the master database
    master_db = couch[db_name]
else:
    db = couch.create(db_name)
    print(f"Database '{db_name}' created successfully.")
    # Get the master database
    master_db = db

# Connect to MariaDB relational database
db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "mariadb",
    "database": "MusicCompDB_DIM"
}

conn = mariadb.connect(**db_config)
cursor = conn.cursor(dictionary=True)

# Port data for the age group dimension
cursor.execute("SELECT * FROM AGEGROUP_DIM")
age_groups = cursor.fetchall()

for age_group in age_groups:
    # Convert Decimal values to strings
    for key, value in age_group.items():
        if isinstance(value, decimal.Decimal):
            age_group[key] = str(value)

    # Create a document for each age group
    doc_id = "age_group:" + str(age_group["AGE_GROUPID"])

    dimension_doc = {
        "_id": doc_id,
        "age_group": age_group
    }
    master_db.save(dimension_doc)

# Port data for Wicklow to CouchDB
cursor.execute("SELECT * FROM votes_fact WHERE countyid = 17")
wicklow_votes = cursor.fetchall()

for vote in wicklow_votes:
    # Convert Decimal values to strings
    for key, value in vote.items():
        if isinstance(value, decimal.Decimal):
            vote[key] = str(value)
    
    vote_id = vote["VOTE_ID"]  # Or some other unique attribute of the vote

    # Add the partition key to the ID
    doc_id = "wicklow:" + str(vote_id)

    print(f"Porting document {doc_id} to CouchDB")

    fact_doc = {
        "_id": doc_id,
        "age_group": "age_group:" + str(vote["AGE_GROUPID"]),
        "county": "Wicklow",
        "votes": [vote]
    }
    master_db.save(fact_doc)

# Port data for Kerry to CouchDB
cursor.execute("SELECT * FROM votes_fact WHERE countyid = 5")
kerry_votes = cursor.fetchall()

for vote in kerry_votes:
    # Convert Decimal values to strings
    for key, value in vote.items():
        if isinstance(value, decimal.Decimal):
            vote[key] = str(value)

    vote_id = vote["VOTE_ID"]  # Or some other unique attribute of the vote

    # Add the partition key to the ID
    doc_id = "kerry:" + str(vote_id)

    print(f"Porting document {doc_id} to CouchDB")

    fact_doc = {
        "_id": doc_id,
        "age_group": "age_group:" + str(vote["AGE_GROUPID"]),
        "county": "Kerry",
        "votes": [vote]
    }
    master_db.save(fact_doc)


# Create a list to store all documents
all_docs = []

# Export all of the documents to a JSON file
for doc in master_db.view('_all_docs'):

    # Get the document
    document = master_db.get(doc['id'])

    # Print status message
    print(f"Writing document {document} to JSON file")

    # Convert decimal values to strings
    for key, value in document.items():
        if isinstance(value, decimal.Decimal):
            document[key] = str(value)

    # Add the document to the list
    all_docs.append(document)

# Write all documents to the JSON file
with open('C20703429_MusicCompDB.json', 'w') as json_file:
    json.dump(all_docs, json_file, indent=0)

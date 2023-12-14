import mariadb
import couchdb
import json
import decimal

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  

# Set database name
db_name = "c20703429_musiccompdb"  

# Options flags
port_data = False
save_to_json = False
create_global_query = True
execute_global_query = False
create_partition_query = True

if db_name in couch:

    # Get the database
    db = couch[db_name]

else:

    # Create the database
    db = couch.create(db_name)
    print(f"Database '{db_name}' created successfully.")

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
if(port_data):
    cursor.execute("SELECT * FROM AGEGROUP_DIM")
    age_groups = cursor.fetchall()

    for age_group in age_groups:

        # Convert decimal values to strings
        for key, value in age_group.items():
            if isinstance(value, decimal.Decimal):
                age_group[key] = str(value)

        # Create a document for each age group
        doc_id = "age_group:" + str(age_group["AGE_GROUPID"])

        dimension_doc = {
            "_id": doc_id,
            "age_group": age_group
        }
        db.save(dimension_doc)

    # Port data for Wicklow to CouchDB
    cursor.execute("SELECT * FROM votes_fact WHERE countyid = 17")
    wicklow_votes = cursor.fetchall()

    for vote in wicklow_votes:

        # Convert decimal values to strings
        for key, value in vote.items():
            if isinstance(value, decimal.Decimal):
                vote[key] = str(value)
        
        vote_id = vote["VOTE_ID"]

        # Add the partition key to the ID
        doc_id = "wicklow:" + str(vote_id)

        print(f"Porting document {doc_id} to CouchDB")

        fact_doc = {
            "_id": doc_id,
            "age_group": "age_group:" + str(vote["AGE_GROUPID"]),
            "county": "Wicklow",
            "votes": [vote]
        }
        db.save(fact_doc)

    # Port data for Kerry to CouchDB
    cursor.execute("SELECT * FROM votes_fact WHERE countyid = 5")
    kerry_votes = cursor.fetchall()

    for vote in kerry_votes:

        # Convert decimal values to strings
        for key, value in vote.items():
            if isinstance(value, decimal.Decimal):
                vote[key] = str(value)

        vote_id = vote["VOTE_ID"]

        # Add the partition key to the ID
        doc_id = "kerry:" + str(vote_id)

        print(f"Porting document {doc_id} to CouchDB")

        fact_doc = {
            "_id": doc_id,
            "age_group": "age_group:" + str(vote["AGE_GROUPID"]),
            "county": "Kerry",
            "votes": [vote]
        }
        db.save(fact_doc)

if(create_global_query):

    design_doc = {
        "_id": "_design/votes_by_age_group_and_county",
        "options": {"partitioned": False},  # This makes the view global
        "views": {
            "votes_by_age_group_and_county": {
                "map": """
                function (doc) {
                    if (doc._id.startsWith('kerry:')) {
                        var county = doc._id.split(':')[1];  // Extract the county from the _id
                        var votes = doc.votes;

                        for (var i = 0; i < votes.length; i++) {
                            var vote = votes[i];
                            var ageGroup = vote.age_group;  

                            // Emit the vote count for each age group and county
                            emit({ ageGroup: ageGroup, county: county }, { count: votes.vote });  
                        }
                    }
                }""",
                "reduce": """
                function (keys, values) {
                    var totalCount = 0;

                    for (var i = 0; i < values.length; i++) {
                        totalCount += values[i].count;
                    }

                    return { count: totalCount };
                }"""
            }
        }
    }

    # Save the design document
    db.save(design_doc)



if(execute_global_query):
    # Query the view for each partition
    for county in ['kerry', 'wicklow']:
        for row in db.view('_design/my_design_doc/_view/votes_by_age_group_and_county', partition=county, group=True):
            age_group_description = row.key[1]
            total_votes = row.value

            # Print the county, age group description, and total votes
            print(county, age_group_description, total_votes)

if(create_partition_query):
    # Create a design document
    design_doc = {
        "_id": "_design/partition_query",
        "views": {
            "my_view": {
                "map": """
                    function(doc) {
                        if(doc.votes) {
                            emit(doc._id, doc.votes);
                        }
                    }
                """
            }
        }
    }
    db.save(design_doc)




if(save_to_json):
    
    # Create a list to store all documents
    all_docs = []

    # Export all of the documents to a JSON file
    for doc in db.view('_all_docs'):

        # Get the document
        document = db.get(doc['id'])

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
        json.dump(all_docs, json_file, indent=4)

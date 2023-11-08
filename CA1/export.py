import mariadb
import couchdb
from datetime import datetime

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  
db_name = "music_comp_db"  # Replace with your desired CouchDB database name

couch.delete(db_name)  # Delete the database if it already exists

try:
    db = couch.create(db_name)
    print(f"Database '{db_name}' created successfully.")
except couchdb.PreconditionFailed as e:
    if e.args[0].status_code == 412:
        db = couch[db_name]
        print(f"Database '{db_name}' already exists.")
    else:
        raise

# Connect to your MariaDB relational database
db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "mariadb",
    "database": "MusicCompDB_DIM"
}

conn = mariadb.connect(**db_config)
cursor = conn.cursor(dictionary=True)

# Define tables to export
tables = ['AGEGROUP_DIM', 'COUNTY_DIM', 'EDITION_DIM', 'PARTICIPANTS_DIM', 'VIEWERS_DIM', 'VIEWERCATEGORY_DIM', 'VOTES_FACT']

for table in tables:
    # Retrieve data from MariaDB
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()

    # Insert data into CouchDB
    for row in rows:
        # Build CouchDB document structure with a timestamp to ensure uniqueness
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        document_id = f"{table.lower()}_{row.get(f'{table.upper()}_ID', row.get(f'{table.upper()}ID'))}_{timestamp}"
        document = {"_id": document_id, "type": table.lower()}

        # Add table-specific fields
        if table == 'AGEGROUP_DIM':
            document.update({
                "age_groupid": row['AGE_GROUPID'],
                "age_group_desc": row['AGE_GROUP_DESC']
            })
        elif table == 'COUNTY_DIM':
            document.update({
                "countyid": row['COUNTYID'],
                "countyname": row['COUNTYNAME']
            })
        elif table == 'EDITION_DIM':
            document.update({
                "edyear": row['EDYEAR'],
                "edpresenter": row['EDPRESENTER']
            })
        elif table == 'PARTICIPANTS_DIM':
            document.update({
                "partname": row['PARTNAME'],
                "countyid": row['COUNTYID']
            })
        elif table == 'VIEWERS_DIM':
            document.update({
                "viewerid": row['VIEWERID'],
                "age_groupid": row['AGE_GROUPID'],
                "countyid": row['COUNTYID']
            })
        elif table == 'VIEWERCATEGORY_DIM':
            document.update({
                "catid": row['CATID'],
                "catname": row['CATNAME']
            })
        elif table == 'VOTES_FACT':
            viewer_id = row.get('VIEWERID', '')
            age_group_id = row.get('AGE_GROUPID', '')
            county_id = row.get('COUNTYID', '')
            participant_name = row.get('PARTNAME', '')
            vote_category_id = row.get('VOTE_CATEGORY', '')
            vote_category_name = row.get('CATNAME', '')

            viewer_document_id = f"viewers_dim_{viewer_id}"
            age_group_document_id = f"agegroup_dim_{age_group_id}"
            county_document_id = f"county_dim_{county_id}"
            participant_document_id = f"participants_dim_{participant_name}"
            vote_category_document_id = f"viewercategory_dim_{vote_category_id}"

            document.update({
                "vote_id": row['VOTE_ID'],
                "viewer": {"id": viewer_id, "age_groupid": age_group_id, "countyid": county_id},
                "edition": {"edyear": row['EDYEAR'], "edpresenter": row.get('ED_PRESENTER', '')},
                "participant": {"partname": participant_name, "countyid": county_id},
                "vote_category": {"catid": vote_category_id, "catname": row.get('CATNAME', '')},
                "votemode": row.get('VOTEMODE', ''),
                "vote": row.get('VOTE', None)
            })

            print(f"Exporting {table} document with ID {document_id}")

            # Link the nested documents
            age_group_document_id = f"agegroup_dim_{age_group_id}"
            county_document_id = f"county_dim_{county_id}"
            viewer_document_id = f"viewers_dim_{viewer_id}"
            participant_document_id = f"participants_dim_{participant_name}"
            vote_category_document_id = f"viewercategory_dim_{vote_category_id}"

            # Save the linked documents if not already present
            if age_group_document_id not in db:
                print(f"AGEGROUP_DIM document with ID {age_group_document_id} not found. Creating...")
                age_group_document = {
                    "_id": age_group_document_id,
                    "type": "agegroup_dim",
                    "age_groupid": age_group_id,
                    "age_group_desc": f"Age Group {age_group_id}"
                }
                db.save(age_group_document)

            if county_document_id not in db:
                print(f"COUNTY_DIM document with ID {county_document_id} not found. Creating...")
                county_document = {
                    "_id": county_document_id,
                    "type": "county_dim",
                    "countyid": county_id,
                    "countyname": f"County {county_id}"
                }
                db.save(county_document)

            if viewer_document_id not in db:
                print(f"VIEWERS_DIM document with ID {viewer_document_id} not found. Creating...")
                viewer_document = {
                    "_id": viewer_document_id,
                    "type": "viewers_dim",
                    "viewerid": viewer_id,
                    "age_groupid": age_group_id,
                    "countyid": county_id
                }
                db.save(viewer_document)

            if participant_document_id not in db:
                print(f"PARTICIPANTS_DIM document with ID {participant_document_id} not found. Creating...")
                participant_document = {
                    "_id": participant_document_id,
                    "type": "participants_dim",
                    "partname": participant_name,
                    "countyid": county_id
                }
                db.save(participant_document)

            if vote_category_document_id not in db:
                print(f"VIEWERCATEGORY_DIM document with ID {vote_category_document_id} not found. Creating...")
                vote_category_document = {
                    "_id": vote_category_document_id,
                    "type": "viewercategory_dim",
                    "catid": vote_category_id,
                    "catname": vote_category_name
                }
                db.save(vote_category_document)


        # Insert the document into CouchDB
        db.save(document)

# Close the database connections
cursor.close()
conn.close()
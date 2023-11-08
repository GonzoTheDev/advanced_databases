import mariadb
import couchdb
import uuid
from datetime import datetime

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  
db_name = "music_comp_db"  # Replace with your desired CouchDB database name

if db_name in couch:
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

# Define the design document with all views
design_doc = {
    "_id": "_design/views",
    "views": {
        "by_county_and_participant": {
            "map": "function (doc) { if (doc.type === 'votes_fact' && doc.edition.edyear === 2022 && doc.vote_category.catid === 2) { emit([doc.participant.countyname, doc.participant.partname], doc.vote); }}"
        },
        "by_viewer": {
            "map": "function (doc) { if (doc.type === 'viewers_dim') { emit(doc._id, doc.age_groupid, doc.viewerid); }}"
        },
        "by_county": {
            "map": "function (doc) { if (doc.type === 'county_dim') { emit(doc._id, doc.countyid, doc.countyname); }}"
        },
        "by_edition": {
            "map": "function (doc) { if (doc.type === 'edition_dim') { emit(doc._id, doc.edyear, doc.edpresenter); }}"
        },
        "by_participants": {
            "map": "function (doc) { if (doc.type === 'participants_dim') { emit(doc._id, doc.partname, doc.countyid); }}"
        },
        "by_viewercategory": {
            "map": "function (doc) { if (doc.type === 'viewercategory_dim') { emit(doc._id, doc.catid, doc.catname); }}"
        }
    }
}

# Save the design document
try:
    db[design_doc["_id"]] = design_doc
    print("Design document created.")
except couchdb.ResourceConflict:
    print("Design document already exists.")

    
def get_document_id_by_key(db, key, type, view):
    search_key = type + "_" + str(key)
    view_result = db.view('views/' + view, key=search_key)
    
    if view_result:
        return view_result.rows[0].id
    else:
        return None

def get_countyname(db, key, type, view):
    search_key = type + "_" + str(key)
    view_result = db.view('views/' + view, key=search_key)
    
    if view_result:
        return view_result.rows[0].countyname
    else:
        return None


# Define tables to export
#  
tables = ['AGEGROUP_DIM', 'COUNTY_DIM', 'EDITION_DIM', 'PARTICIPANTS_DIM', 'VIEWERS_DIM', 'VIEWERCATEGORY_DIM', 'VOTES_FACT']

for table in tables:
    # Retrieve data from MariaDB
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()

    # Insert data into CouchDB
    for row in rows:
        # Build CouchDB document structure with a timestamp to ensure uniqueness
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        
        viewer_id = row.get('VIEWERID', '')
        age_group_id = row.get('AGE_GROUPID', '')
        age_group_desc = row.get('AGE_GROUP_DESC', '')
        ed_year = row.get('EDYEAR', '')
        ed_presenter = row.get('EDPRESENTER', '')
        county_id = row.get('COUNTYID', '')
        county_name = row.get('COUNTYNAME', '')
        participant_name = row.get('PARTNAME', '')
        viewer_category_id = row.get('CATID', '')
        viewer_category_name = row.get('CATNAME', '')
        vote_category_id = row.get('VOTE_CATEGORY', '')
        vote_mode = row.get('VOTEMODE', '')
        vote = row.get('VOTE', '')
        vote_id = row.get('VOTE_ID', '')

        viewer_document_id = f"viewers_dim_{viewer_id}"
        age_group_document_id = f"agegroup_dim_{age_group_id}"
        county_document_id = f"county_dim_{county_id}"
        participant_document_id = f"participants_dim_{participant_name.lower()}"
        viewer_category_document_id = f"viewercategory_dim_{viewer_category_id}"
        edition_document_id = f"edition_dim_{ed_year}_{ed_presenter}"
        votes_fact_document_id = f"votes_fact_{vote_id}"
        
        if table == 'AGEGROUP_DIM':
            document = {"_id": age_group_document_id, "type": table.lower()}
        if table == 'COUNTY_DIM':
            document = {"_id": county_document_id, "type": table.lower()}
        if table == 'EDITION_DIM':
            document = {"_id": edition_document_id, "type": table.lower()}
        if table == 'PARTICIPANTS_DIM':
            document = {"_id": participant_document_id, "type": table.lower()}
        if table == 'VIEWERS_DIM':
            document = {"_id": viewer_document_id, "type": table.lower()}
        if table == 'VIEWERCATEGORY_DIM':
            document = {"_id": viewer_category_document_id, "type": table.lower()}
        if table == 'VOTES_FACT':
            document = {"_id": votes_fact_document_id, "type": table.lower()}


        # Add table-specific fields
        if table == 'AGEGROUP_DIM':
            document.update({
                "_id": age_group_document_id,
                "age_groupid": age_group_id,
                "age_group_desc": age_group_desc
            })
            print(f"Exporting {table} document with ID {age_group_document_id}")
        elif table == 'COUNTY_DIM':
            document.update({
                "_id": county_document_id,
                "countyid": county_id,
                "countyname": county_name
            })
            print(f"Exporting {table} document with ID {county_document_id}")
        elif table == 'EDITION_DIM':
            document.update({
                "_id": edition_document_id,
                "edyear": ed_year,
                "edpresenter": ed_presenter
            })
            print(f"Exporting {table} document with ID {edition_document_id}")
        elif table == 'PARTICIPANTS_DIM':
            document.update({
                "_id": participant_document_id,
                "partname": participant_name,
                "countyid": county_id
            })
            pt_countyname_document_id = get_countyname(db, county_id, 'county_dim', 'by_viewer')
            if(pt_countyname_document_id):
                document['countyname'] = pt_countyname_document_id
            print(f"Exporting {table} document with ID {participant_document_id}")
        elif table == 'VIEWERS_DIM':
            document.update({
                "_id": viewer_document_id,
                "viewerid": viewer_id,
                "age_groupid": age_group_id,
                "countyid": county_id
            })
            print(f"Exporting {table} document with ID {viewer_document_id}")
        elif table == 'VIEWERCATEGORY_DIM':
                document.update({
                    "_id": viewer_category_document_id,
                    "catid": viewer_category_id,
                    "catname": viewer_category_name
                })
                print(f"Exporting {table} document with ID {viewer_category_document_id}")
        elif table == 'VOTES_FACT':
            document.update({
                "_id": votes_fact_document_id,
                "vote_id": row['VOTE_ID'],
                "viewer": {"id": viewer_id, "age_groupid": age_group_id, "countyid": county_id},
                "edition": {"edyear": row['EDYEAR']},
                "participant": {"partname": participant_name, "countyid": county_id},
                "vote_category": {"catid": vote_category_id},
                "votemode": vote_mode,
                "vote": vote
            })

            vf_viewer_document_id = get_document_id_by_key(db, viewer_id, 'viewers_dim', 'by_viewer')
            if(vf_viewer_document_id):
                document['viewer']['_id'] = vf_viewer_document_id
            
            vf_edition_document_id = get_document_id_by_key(db, ed_year, '', 'by_edition')
            if(vf_edition_document_id):
                document['edition']['_id'] = vf_edition_document_id

            vf_participant_document_id = get_document_id_by_key(db, participant_name.lower(), 'participants_dim', 'by_participants')
            if(vf_participant_document_id):
                document['participant']['_id'] = vf_participant_document_id

            vf_vote_category_document_id = get_document_id_by_key(db, vote_category_id, 'viewercategory_dim', 'by_viewercategory')
            if(vf_vote_category_document_id):
                document['vote_category']['_id'] = vf_vote_category_document_id
                

            print(f"Exporting {table} document with ID {votes_fact_document_id}")

        # Insert the document into CouchDB
        db.save(document)




# Close the database connections
cursor.close()
conn.close()
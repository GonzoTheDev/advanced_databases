import couchdb

# Set up the connection to CouchDB
couch = couchdb.Server("http://admin:couchdb@127.0.0.1:5984")  
db_name = "music_comp_db"  
db = couch[db_name]

# Mango query to return all documents from the votes_fact collection where the votemode is Instagram and the edition is 2022
query_18_24 = {
    "selector": {
        "type": "votes_fact",
        "viewer": {"age_groupid": 1},
        "votemode": "Instagram",
        "edition": {"edyear": 2022}
    },
    "limit": 1000
}
print("Returning vote counts for each age group who voted on Instagram in 2022")
result_18_24 = db.find(query_18_24)
count_18_24 = 0
for row in result_18_24:
    count_18_24 = count_18_24 +1

print("18-24 Age Group:", count_18_24)

query_25_30 = {
    "selector": {
        "type": "votes_fact",
        "viewer": {"age_groupid": 2},
        "votemode": "Instagram",
        "edition": {"edyear": 2022}
    },
    "limit": 1000
}
result_25_30 = db.find(query_25_30)
count_25_30 = 0
for row in result_25_30:
    count_25_30 = count_25_30 +1

print("25-30 Age Group:", count_25_30)

query_31_49 = {
    "selector": {
        "type": "votes_fact",
        "viewer": {"age_groupid": 3},
        "votemode": "Instagram",
        "edition": {"edyear": 2022}
    },
    "limit": 1000
}
result_31_49 = db.find(query_31_49)
count_31_49 = 0
for row in result_31_49:
    count_31_49 = count_31_49 +1

print("31-49 Age Group:", count_31_49)

query_gt50 = {
    "selector": {
        "type": "votes_fact",
        "viewer": {"age_groupid": 4},
        "votemode": "Instagram",
        "edition": {"edyear": 2022}
    },
    "limit": 1000
}
result_gt50 = db.find(query_gt50)
count_gt50 = 0
for row in result_gt50:
    count_gt50 = count_gt50 +1

print("50+ Age Group:", count_gt50)

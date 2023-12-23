curl -X PUT http://admin:couchdb@localhost:5984/c20703429_musiccompdb_replica?partitioned=true

curl -X PUT http://admin:couchdb@localhost:5984/c20703429_musiccompdb?partitioned=true

curl -X GET http://admin:couchdb@127.0.0.1:5984/c20703429_musiccompdb/_all_docs?include_docs=TRUE
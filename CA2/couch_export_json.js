var couchimport = require('couchimport');
var opts = { delimiter: ",", url: "http://admin:couchdb@127.0.0.1:5984", database: "c20703429_musiccompdb", type: "json" };


couchimport.exportFile("output.json", opts, function(err, data) {
    console.log("done",err,data);
 });
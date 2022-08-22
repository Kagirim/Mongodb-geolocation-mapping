var MongoClient =  require("mongodb").MongoClient;

MongoClient.connect("mongodb://localhost:27017", function (err, client){
    const db = client.db("yourNetID_mod14");
    if (err) throw err;
    const county_collection = db.collection("counties");
    const chipotle_collection = db.collection("chipotle");

    chipotle_collection.dropIndexes();
    county_collection.dropIndexes();
    
    county_collection.createIndex({ "Geo Shape": "2dsphere" }, { name: "countyGeoShape"});
    county_collection.createIndex({ "Geo Point": "2dsphere" }, { name: "countyGeoPoint"});
    chipotle_collection.createIndex({ "geo point": "2dsphere" }, { name: "chipotleGeoPoint"});

    client.close();

  });
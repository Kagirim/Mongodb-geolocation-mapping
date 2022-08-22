var MongoClient =  require("mongodb").MongoClient;

MongoClient.connect("mongodb://localhost:27017", function (err, client){
    const db = client.db("yourNetID_mod14");
    if (err) throw err;
    
    const chipotle_collection = db.collection("chipotle");
    const counties_collection = db.collection("counties");

    counties_collection.find({}, { _id: 0, NAME: 1, "Geo Shape": 1}).toArray(function(err, result){
        if (err) throw err;
        for (let i in result) {
            var locations_array = db.chipotle.aggregate([
                {
                  "$search": {
                    "geoWithin": {
                      "geometry": {
                        "type": "Polygon",
                        "coordinates": i["Geo Shape"]["coordinates"]
                      },
                      "path": "chipotle['geo point']"
                    }
                  }
                }
              ]).toArray();
            
            counties_collection.updateOne( { "NAME": county}, {$set: {"STORES": locations_array}});
            };

    });
        
    client.close();

  });
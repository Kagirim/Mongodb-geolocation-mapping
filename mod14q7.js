var MongoClient =  require("mongodb").MongoClient;

MongoClient.connect("mongodb://localhost:27017", function (err, client){
    const db = client.db("yourNetID_mod14");
    if (err) throw err;
    const county_collection = db.collection("counties");
    const chipotle_collection = db.collection("chipotle");

    var most_chipotle_locations = ""

    var top_chipotle_location = county_collection.find({name: most_chipotle_locations}, {_id: 0, name: 1, state: 1, "Geo Point": 1})["Geo Point"]["coordinates"]
    
    //county_collection.find({ coordinates: {$nearSphere: { $geometry: { type: "Point", coordinates: top_chipotle_location }}}})
    var closest_chipotle_stores = county_collection.aggregate([
        {
            $geoNear: {
                near: { type: "Point", coordinates: top_chipotle_location},
                spherical: true, 
                distanceField: "dist.calculated",
                includeLocs: "dist.location"
            }
        }
    ]).sort({"dist.calculated": 1}).limit(3);

    console.log(closest_chipotle_stores)
    client.close();

  });
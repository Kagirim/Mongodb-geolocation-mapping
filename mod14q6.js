var MongoClient =  require("mongodb").MongoClient;

MongoClient.connect("mongodb://localhost:27017", function (err, client){
    const db = client.db("yourNetID_mod14");
    if (err) throw err;
    
    var top_five = db.counties.aggregate([
        {
           $project: {
              County: 1,
              STATE: 1,
              "Number of Chipotles": { $cond: { if: { $isArray: "$STORES" }, then: { $size: "$STORES" }, else: "NA"} }
           }
        },
        {"$sort": {"Number of Chipotles": -1}},
        { $limit: 5}
     ] );
    
    console.log(top_five);
    client.close();
});

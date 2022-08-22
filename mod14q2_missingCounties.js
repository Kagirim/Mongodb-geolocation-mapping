var MongoClient =  require("mongodb").MongoClient;

MongoClient.connect("mongodb://localhost:27017", function (err, client){
    const db = client.db("yourNetID_mod14");
    if (err) throw err;

    db.collection("countyids").find( { } ).toArray(function(err, result) {
        if (err) throw err;
        countyns_obj = result;
    });
    db.collection("counties").find( { } ).toArray(function(err, result) {
        if (err) throw err;
        counties_dict = result;
        client.close();  
  });
});
//console.log(countyns_dict);

for (let i in counties_dict) {
    if  (Object.values(countyns_dict).includes(i["COUNTYNS"])){
        console.log(i["COUNTYNS"]);
    };
};

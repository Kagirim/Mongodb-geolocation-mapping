import csv, json
import re

csv.field_size_limit(1000000)

step = 100
batchSize = 200
numCounties = 3233
coordGroupSize = 100

with open("chipotle_stores.csv","r", encoding="utf-8") as f:
    csv_reader = csv.reader(f,delimiter=';')
    
    count = 0
    headings = []
    documentsArray = []
    
    for row in csv_reader:
        count += 1
        
        if count%step == 0:
            print("{} out of {}".format(count, numCounties))

        if count == 1:
            headings = str(row).strip("[]'").split(",")
        else:
            row = str(row).replace("\"", "").strip("[\']").split(",")
            document_dict = {}
            latitude_index = headings.index("latitude")
            longitude_index = headings.index("longitude")
            document_dict["geo point"] = {"type":"Point","coordinates": [float(row[longitude_index]), float(row[latitude_index])]}
            for index, value in enumerate(headings):
                if value != "latitude" and value != "longitude":
                    document_dict[value] = row[index]

            
            documentsArray.append(json.dumps(document_dict))
                
#insertScript += ",{writeConcern: {w:0}, ordered: false})\n"
#insertScript += "console.log('$count out of $numCounties)\n"
insertScript = ""
insertScript += """
const mongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017/yourNetID_mod14';

mongoClient.connect(url, { useNewUrlParser: true }, (err, client) => {
    if (err) throw err;
    console.log("Database created")

    client.collection("chipotle").drop(function(err, delOK) {
        if (err) throw err;
        if (delOK) console.log("Collection deleted");

    client.createCollection("chipotle", function(err, result) {
        if (err) throw err;
        console.log("Collection "chipotle" created!");

    );\n
"""
#insertScript += "client.collection('counties').insertMany({})\n".format(documentsArray)

insertScript += """

})
});
"""

with open("mod14q3_insertChipotle.js","w") as f:
    pass
    #f.write(insertScript)
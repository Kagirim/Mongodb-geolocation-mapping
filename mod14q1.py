import csv, json
import re

csv.field_size_limit(1000000)

step = 100
batchSize = 200
numCounties = 3233
coordGroupSize = 100

with open("us-county-boundaries.csv","r", encoding="utf-8") as f:
    csv_reader = csv.reader(f,delimiter=';')
    
    count = 0
    headings = []
    documentsArray = []
    
    for row in csv_reader:
        count += 1
        
        if count%step == 0:
            print("{} out of {}".format(count, numCounties))
        
        rowDict = {}
        if count == 1:
            headings = row
        else:
            for index,value in enumerate(row):
                if headings[index] == "Geo Shape":
                    geoShapeDict = json.loads(row[index])
                    coordsList = geoShapeDict["coordinates"]
                    pattern = '((\[\-\d+\.\d+\,\ \d+\.\d+\]\,\ ){' + str(coordGroupSize) + '})'
                    coordsList = re.sub(pattern, r'\1\n', str(coordsList))
                    rowDict[headings[index]] = {"coordinates": list(coordsList), "type": geoShapeDict["type"]}

                elif headings[index] == "Geo Point":
                    coordsList = [coord for coord in value.split(",")]
                    geoPoint = [float(coordsList[1]), float(coordsList[0])]
                    rowDict[headings[index]] = {"type":"Point","coordinates":geoPoint}
                else:
                    rowDict[headings[index]] = value

                documentsArray.append(json.dumps(rowDict))
                
#insertScript += ",{writeConcern: {w:0}, ordered: false})\n"
#insertScript += "console.log('$count out of $numCounties)\n"
insertScript = ""
insertScript += """
const mongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017/yourNetID_mod14';

mongoClient.connect(url, { useNewUrlParser: true }, (err, client) => {
    if (err) throw err;
    console.log("Database created")

    client.collection("counties").drop(function(err, delOK) {
        if (err) throw err;
        if (delOK) console.log("Collection deleted");

    client.createCollection("counties", function(err, result) {
        if (err) throw err;
        console.log("Collection "counties" created!");


    );\n
"""
#insertScript += "client.collection('counties').insertMany({})\n".format(documentsArray)

insertScript += """
client.close();
})
});
"""

with open("mod14q1_insertCounties.js","w") as f:
    pass
    #f.write(insertScript)
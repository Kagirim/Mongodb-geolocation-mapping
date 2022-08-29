import csv, json
import re
import pandas as pd

df = pd.DataFrame()
csv.field_size_limit(1000000)
rows = []

with open("us-county-boundaries.csv","r", encoding="utf-8") as f:
    csv_reader = csv.reader(f,delimiter=';')
    count = 0
    for row in csv_reader:
        count += 1
        if count % 100 == 0:
            print("{} out of {}".format(count, 3233))
        if count == 1:
            headings = row

        else:
            rows.append(row)

df = pd.DataFrame(rows, columns = headings)

# Geo Shape
geo_shape = df["Geo Shape"]
for i in geo_shape:
    geo_shape_dict = json.loads(i)
    shape_coords = geo_shape_dict["coordinates"]
    pattern = '((\[\-\d+\.\d+\,\ \d+\.\d+\]\,\ ){' + str(100) + '})'
    shape_coords = re.sub(pattern, r'\1\n', str(shape_coords))
    geo_shape = {"coordinates": shape_coords, "type": geo_shape_dict["type"]}

# Geo Point
geo_points_list = []
geo_point_column = df["Geo Point"]
row_count = -1
for i in geo_point_column:
    row_count += 1
    geo_point_coords = [float(coord) for coord in i.split(",")]
    geo_point_column[row_count] = {"type":"Point","coordinates": geo_point_coords}


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
insertScript += "client.collection('counties').insertMany({})\n".format(df.to_dict("records"))

insertScript += """
client.close();
})
});
"""

with open("mod14q1_insertCounties.js","w") as f:
    f.write(insertScript)
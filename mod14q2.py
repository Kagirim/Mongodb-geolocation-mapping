import csv
csv.field_size_limit(1000000)

with open("us-county-boundaries.csv","r", encoding="utf-8") as f:
    csv_reader = csv.reader(f, delimiter=';')
    row_count = 0
    county_ids_list = []
    for row in csv_reader:
        county_id_dict = {}
        row_count += 1
        if row_count == 1:
            column_names = row
        else:
            county_index = column_names.index("COUNTYNS")
            county_name = row[column_names.index("NAME")]
            county_id_dict["COUNTYN"S] = row[county_index]
            county_ids_list.append(county_id_dict) 


insertScript = ""
insertScript += """
const mongoClient = require('mongodb').MongoClient;

const url = 'mongodb://localhost:27017';

mongoClient.connect(url, { useNewUrlParser: true }, (err, client) => {
    if (err) throw err;
    const db = client.db("yourNetID_mod14");

    db.createCollection("countyids");
    \
"""

insertScript += "db.collection('countyids').insertMany({})\n".format(str(county_ids_list))


insertScript += """
client.close();
    })
});
"""
with open("mod14q1_insertCountryIds.js","w") as f:
    pass
    #f.write(insertScript)
import csv, json
import re
import pandas as pd
import ast

df = pd.DataFrame()
csv.field_size_limit(1000000)
rows = []

with open("us-county-boundaries.csv","r", encoding="utf-8") as f:
    csv_reader = csv.reader(f,delimiter=';')
    count = 0
    for row in csv_reader:
        count += 1
        
        if count == 1:
            headings = row

        else:
            rows.append(row)

df = pd.DataFrame(rows, columns = headings)

# Geo Shape
geo_shape = df["Geo Shape"]
print_count = 0
shape_row_count = -1
for i in geo_shape:
    shape_row_count += 1
    geo_shape_dict = json.loads(i)
    shape_coords = geo_shape_dict["coordinates"]
    pattern = '((\[\-\d+\.\d+\,\ \d+\.\d+\]\,\ ){' + str(100) + '})'
    shape_coords = re.sub(pattern, r'\1\n', str(shape_coords))
    geo_shape[shape_row_count] = {"type": geo_shape_dict["type"], "coordinates": ast.literal_eval(shape_coords)}
    
# Geo Point
geo_points_list = []
geo_point_column = df["Geo Point"]
row_count = -1
for i in geo_point_column:
    row_count += 1
    if count % 100 == 0:
        print("{} out of {}".format(count, 3233))
    geo_point_coords = [float(coord) for coord in i.split(",")]
    geo_point_column[row_count] = {"type":"Point","coordinates": geo_point_coords}


#insertScript += ",{writeConcern: {w:0}, ordered: false})\n"
#insertScript += "console.log('$count out of $numCounties)\n"
insertScript = ""
insertScript += """
const { MongoClient } = require('mongodb');

async function main() {
    const url = 'mongodb://localhost:27017/mydb';
    const client = new MongoClient(url);
    try {
        await client.connect();
        await create_collection(client, "mydb", "counties");
        await insert(client, "mydb", "counties");

    } catch(e) {
        console.error(e);

    } finally {
        await client.close();

    }
}
main().catch(console.error);

async function create_collection(client, mydb, collection_name) {
    try {
        //const collections_list = await client.db(mydb).getCollectionNames();
        await client.db(mydb).collection(collection_name).drop();
    } catch(err) {
        //console.log(err);
    } finally {
        await client.db(mydb).createCollection(collection_name);
    }
}

"""
insertScript += "async function insert(client, mydb, collection_name) { "
insertScript += "await client.db(mydb).collection(collection_name).insertMany({doc_array});".format(doc_array = df.to_dict("records"))
insertScript += "}"
with open("mod14q1_insertCounties.js","w") as f:
    f.write(insertScript)
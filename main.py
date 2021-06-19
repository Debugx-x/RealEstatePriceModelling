import json
import pandas as pd  
import config
import http.client 
import sqlite3
from sqlite3 import Error

conn = http.client.HTTPSConnection("sandbox.repliers.io")

payload = "{}"

headers = {'repliers-api-key': config.api_key}

conn.request("GET", "/listings?class=residential&sortBy=random&resultsPerPage=100&status=A", payload, headers)

res = conn.getresponse()
response = res.read()
data = json.loads(response.decode("utf-8"))
listings = data['listings']

listing_df = pd.json_normalize(listings)

def create_connection():
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect('listings.db')
        return conn
    except Error as e:
        print(e)
    return conn

def create_table(conn, table):
    try:
        c = conn.cursor()
        c.execute(table)
    except Error as e:
        print(e)

def insert_listing(conn, listing):
    sql = ''' INSERT OR REPLACE INTO Listings (MLS_Number, List_Price, Street_Name, Street_Number, City, Area, Province,PO_BOX, latitude,longitude)
              VALUES(?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, listing)
    conn.commit()
    return cur.lastrowid

def getdata_by_mls(conn, mls):
    cur = conn.cursor()
    cur.execute("SELECT * FROM Listings WHERE MLS_Number=?", [mls])

    rows = cur.fetchall()
    return rows

def get_listings(propertydata):
    link = "/listings?resultsPerPage=100&sortBy=soldPriceDesc&area=" + propertydata[0][5]
    conn.request("GET", link, payload, headers)
    res1 = conn.getresponse()
    response1 = res1.read()
    data1 = json.loads(response1.decode("utf-8"))
    Arealisting = data1['listings']
    return Arealisting

def display_listings(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM Listings")
    rows = cur.fetchall()
    for row in rows:
        print("MLS Number: ", row[0], " Price: ", "$" + str(row[1]))
        print("Address: ", str(row[3]) + " " + row[2] + " " + row[4] + " " + row[6] + " " + row[7])
        print()
'''
def main():
    conn = create_connection()
    if conn is not None:
        table = """CREATE TABLE IF NOT EXISTS Listings (
                        MLS_Number text PRIMARY KEY,
                        List_Price real NOT NULL,
                        Street_Name text NOT NULL,
                        Street_Number integer NOT NULL,
                        City text NOT NULL,
                        Area text NOT NULL,
                        Province text NOT NULL,
                        PO_BOX text NOT NULL,
                        latitude real NOT NULL,
                        longitude real NOT NULL);
                        """
        create_table(conn, table)
    else:
        print("Error! cannot create the database connection.")
    i = 0
    while i < 15:
        listing = (listings[i]['mlsNumber'],
                   listings[i]['listPrice'],
                   listings[i]['address']['streetName'],
                   listings[i]['address']['streetNumber'],
                   listings[i]['address']['city'],
                   listings[i]['address']['area'],
                   listings[i]['address']['state'],
                   listings[i]['address']['zip'],
                   listings[i]['map']['latitude'],
                   listings[i]['map']['longitude'])
        insert_listing(conn, listing)
        i += 1
    display_listings(conn)

if __name__ == '__main__':
    main()'''
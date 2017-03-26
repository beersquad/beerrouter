# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 15:39:08 2017

@author: lsha
"""

import MySQLdb
import json
import falcon
import requests

cursor = None
db = None
api = falcon.API()

api.add_route('/get_food_qty', getFoodQty())

def queryRelationship(category, sumvalue, catselection):

    if category == "Beer":
        selectionType = "brand_name = '" + catselection+"'"
        groupCat ="pos2.product_name"
        col = 2
    else:
        selectionType = "Product_Name = '" + catselection+"'"
        groupCat = "pos2.brand_name"
        col = 3


    query1 = "CREATE TEMPORARY TABLE IF NOT EXISTS postemp AS (SELECT * FROM pos WHERE category_name = '"+category+"' AND " +selectionType+" ORDER BY order_item_id LIMIT 13000); "
    cursor.execute(query1)
    query2 = "CREATE TEMPORARY TABLE IF NOT EXISTS postemp2 AS (SELECT * FROM pos WHERE category_name = '"+ sumvalue+"' AND price <> 0 ORDER BY order_item_id LIMIT 13000); "
    cursor.execute(query2)
    query = "SELECT pos.Product_Name, pos.brand_name, pos2.Product_Name, pos2.brand_name, pos2.price, SUM(pos2.qty) AS Quantity, SUM(pos2.price*pos2.qty) AS Value FROM "
    query = query+ "postemp as pos INNER JOIN postemp2 AS pos2 ON pos2.order_id = pos.order_id GROUP BY "+groupCat+" ORDER BY SUM(pos2.qty) DESC LIMIT 10"
    print(query)
    cursor.execute(query)
    #cursor.execute("SELECT * FROM pos LIMIT 10")
    results = cursor.fetchall()
    d = {}
    arrayx = []
    arrayy= []
    for i in results:
        arrayx.append(i[3])
        arrayy.append(float(i[5]))
    d['x']=arrayx
    d['y']=arrayy
    data_json = json.dumps(d)
    db.close()
    print (data_json)
    payload = {'json_payload': data_json}
    r = requests.post('http://fedora-nyc1.laulabs.net:3000/api/get_food_qty', data=payload)
    print(r.content)

class getFoodQty():
    def on_post(self, req, resp):
        data = json.loads(req.stream.read(req.content_length or 0))
        queryRelationship(data['food'], data['type'], data['category'])


def connectServer():
    global db
    db = MySQLdb.connect(host="fedora-nyc2.laulabs.net",    # your host, usually localhost
                         user="beersquad",         # your username
                         passwd="beersquad123",  # your password
                         db="beersquad")        # name of the data base
    global cursor
    cursor = db.cursor()

def sanitizeData():

    cursor.execute("SELECT VERSION()")
    #data = cursor.fetchone()
    print (cursor.fetchone())

#def topBeer():

#if __name__ == '__main__':
#    connectServer()
#    queryRelationship("Food","Beer","Poutine")
#    #sanitizeData()

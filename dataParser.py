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

def connectServer():
    global db
    db = MySQLdb.connect(host="fedora-nyc2.laulabs.net",    # your host, usually localhost
                         user="beersquad",         # your username
                         passwd="beersquad123",  # your password
                         db="beersquad")        # name of the data base
    global cursor
    cursor = db.cursor()

def queryRelationship(product, category):
    connectServer()

    query1 = "create temporary table if not exists temp_food (select distinct order_id from pos where product_name = %s limit 5000)"
    cursor.execute(query1, [product])
    print(query1)

    query = ("SELECT p.product_name, sum(qty) as quantity FROM pos_clean p inner join temp_food on temp_food.order_id = p.order_id "
             "where p.category_name = %s and p.brand_name in (select name from top_20_pct_beers) group by p.product_name limit 5000")
    print(query)

    cursor.execute(query, [category])

    d = {}
    d['name'], quantity = zip(*cursor.fetchall())
    d['quantity'] = [float(row) for row in quantity]

    data_json = json.dumps(d)

    db.close()

    return data_json
    #payload = {'json_payload': data_json}
    #requests.post('http://fedora-nyc1.laulabs.net:3000/api/get_product_qty', data=payload)

class getProductQty():
    def on_post(self, req, resp):
        content = req.stream.read(req.content_length or 0).decode('utf-8')
        print("content",content)
        data = json.loads(content)
        print("data",data['product'])
        print("data",data['category'])
        resp.body = queryRelationship(data['product'], data['category'])
        print(resp.body)

api.add_route('/get_product_qty', getProductQty())

def sanitizeData():
    cursor.execute("SELECT VERSION()")
    #data = cursor.fetchone()
    print (cursor.fetchone())

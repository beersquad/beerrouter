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

def queryRelationship(filterCategory, filterType, category):
    connectServer()
    
    if category == "Beer":
        selectionType = "brand_name = '" + filterType+"'"
        groupCat ="pos2.product_name"
        col = 2
    else:
        selectionType = "Product_Name = '" + filterType+"'"
        groupCat = "pos2.brand_name"
        col = 3
    
    
    query1 = "CREATE TEMPORARY TABLE IF NOT EXISTS postemp AS (SELECT * FROM pos WHERE category_name = '"+filterCategory+"' AND " +selectionType+" ORDER BY order_item_id LIMIT 8000); "
    cursor.execute(query1)
    query2 = "CREATE TEMPORARY TABLE IF NOT EXISTS postemp2 AS (SELECT * FROM pos WHERE category_name = '"+ category+"' AND price <> 0 ORDER BY order_item_id LIMIT 8000); "
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
        arrayx.append(i[col])
        arrayy.append(float(i[5]))
    d['Type']=arrayx
    d['Quantity']=arrayy  
    data_json = json.dumps(d)
    db.close()

    return data_json

def top10Pairs():
    connectServer()
    query1 = "CREATE TEMPORARY TABLE IF NOT EXISTS postemp AS (SELECT * FROM pos WHERE category_name = 'Beer' ORDER BY order_item_id LIMIT 13000);"
    cursor.execute(query1)
    query2 = "CREATE TEMPORARY TABLE IF NOT EXISTS postemp2 AS (SELECT * FROM pos WHERE category_name = 'Food' AND price <> 0 ORDER BY order_item_id LIMIT 13000);"
    cursor.execute(query2)                                                          
    query = "SELECT pos.brand_name, pos2.Product_Name, SUM(pos2.qty) AS Quantity"
    query = query + "FROM postemp as pos INNER JOIN postemp2 AS pos2 ON pos2.order_id = pos.order_id GROUP BY pos.brand_name, pos2.Product_Name ORDER BY SUM(pos2.qty) DESC LIMIT 10"
    cursor.execute(query)
    results = cursor.fetchall()
    d={}
    arrayx=[]
    arrayy=[]
    arrayz=[]
    for i in results:
        arrayx.append(i[0])
        arrayy.append(i[1])
        arrayz.append(i[2])
    d['Beer'] = arrayx
    d['Food'] = arrayy
    d['Quantity'] = arrayx
    data_json = json.dumps(d)
    db.close()
    
    return data_json
                                                                                                                                                            
class getProductQty():
    def on_post(self, req, resp):
        content = req.stream.read(req.content_length or 0).decode('utf-8')
        print("content",content)
        data = json.loads(content)
        print("data",data['product'])
        print("data",data['category'])
        resp.body = queryRelationship(data['product'], data['filterType'],data['category'])
        print(resp.body)

class getTopPair():
    def on_post(self, req, resp):
        content = req.stream.read(req.content_length or 0).decode('utf-8')
        #print("content",content)
        #data = json.loads(content)
        #print("data",data['product'])
        #print("data",data['category'])
        resp.body = top10Pairs()
        print(resp.body)

api.add_route('/get_product_qty', getProductQty())
api.add_route('/get_top_pair', getTopPair())

def sanitizeData():
    cursor.execute("SELECT VERSION()")
    #data = cursor.fetchone()
    print (cursor.fetchone())

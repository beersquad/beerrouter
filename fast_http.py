import falcon
import json
import MySQLdb
import requests

class Test():
    def on_post(self, req, resp):
        resp.body = bytes(req.stream.read(req.content_length or 0))
        print(resp.body)

class getBeerConsumption():
    def on_post(self, req, resp):
        db = MySQLdb.connect(host="fedora-nyc2.laulabs.net",    # your host, usually localhost
                            user="beersquad",         # your username
                            passwd="beersquad123",  # your password
                            db="beersquad")        # name of the data base
        cursor = db.cursor()

        content = req.stream.read(req.content_length or 0).decode('utf-8')
        print("content:", content, "size", req.content_length)

        data = json.loads(content)
        print(data['bar_type'])

        query = "SELECT brand_name, sum(consumption) as consumption FROM beersquad.draught where bar_type = %s and brand_name in (select brand_name from top_20_pct_beers) group by brand_name order by consumption desc limit 10"
        cursor.execute(query, [data['bar_type']])

        d = {}
        d['name'],consumption = zip(*cursor.fetchall())
        d['consumption'] = [float(row) for row in consumption]

        data_json = json.dumps(d)

        db.close()
        print(d)

        resp.body = data_json

class getBeerTotals():
    def on_post(self, req, resp):
        db = MySQLdb.connect(host="fedora-nyc2.laulabs.net",    # your host, usually localhost
                            user="beersquad",         # your username
                            passwd="beersquad123",  # your password
                            db="beersquad")        # name of the data base
        cursor = db.cursor()

        content = req.stream.read(req.content_length or 0).decode('utf-8')
        print("content", content)

        data = json.loads(content)

        query = ("SELECT p.brand_name, sum(p.qty) AS total_sales FROM beersquad.pos_quality_join j"
                 "INNER JOIN beersquad.pos_clean p on p.bar_product_id = j.bar_product_key WHERE j.external_id = %s"
                 "AND p.brand_name IN (SELECT `name` FROM top_20_pct_beers) GROUP BY brand_name")
        cursor.execute(query, [data['external_id']])

        d = {}
        d['name'],d['count'] = zip(*cursor.fetchall())

        data_json = json.dumps(d)

        db.close()
        print(d)

        resp.body = data_json

class getBeerLocation():
    def on_post(self, req, resp):
        db = MySQLdb.connect(host="fedora-nyc2.laulabs.net",    # your host, usually localhost
                            user="beersquad",         # your username
                            passwd="beersquad123",  # your password
                            db="beersquad")        # name of the data base
        cursor = db.cursor()

        content = req.stream.read(req.content_length or 0).decode('utf-8')
        print("content", content)

        data = json.loads(content)

        query = ("SELECT distinct latitude, longitude FROM beersquad.quality where external_id = %s")
        cursor.execute(query, [data['external_id']])

        latitude, longitude = cursor.fetchone()

        d = {}
        d['latitude'],d['longitude'] = float(latitude), float(longitude)
        data_json = json.dumps(d)

        db.close()
        print(d)

        resp.body = data_json

class getProductQty():
    def on_post(self, req, resp):
        content = req.stream.read(req.content_length or 0)
        print("content", content)
        resp.body = requests.post('http://127.0.0.1:3001/get_product_qty', content).content

class getBarTypes():
    def on_get(self, req, resp):
        db = MySQLdb.connect(host="fedora-nyc2.laulabs.net",    # your host, usually localhost
                            user="beersquad",         # your username
                            passwd="beersquad123",  # your password
                            db="beersquad")        # name of the data base
        cursor = db.cursor()

        content = req.stream.read(req.content_length or 0).decode('utf-8')
        print(content)

        query = 'SELECT distinct bar_type FROM beersquad.draught'
        cursor.execute(query)

        bar_types = {}
        bar_types['bar_types'] = [row[0] for row in cursor.fetchall()]

        data_json = json.dumps(bar_types)

        db.close()

        resp.body = data_json

api = falcon.API()

test = Test()

get_product_qty = getProductQty()
get_beer_totals = getBeerTotals()
get_beer_consumption = getBeerConsumption()
get_beer_location = getBeerLocation()
get_bar_types= getBarTypes()

api.add_route('/test', test)
api.add_route('/get_bar_types', get_bar_types)
api.add_route('/get_beer_consumption', get_beer_consumption)
api.add_route('/get_beer_totals', get_beer_totals)
api.add_route('/get_beer_location', get_beer_location)

api.add_route('/get_product_qty', get_product_qty)

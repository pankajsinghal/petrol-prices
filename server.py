from flask import Flask
import PySQLPool
import logging
import datetime
import datefinder
import requests
import pytz
import json
from bs4 import BeautifulSoup

petrol_url = "https://www.iocl.com/Products/Gasoline.aspx"
diesel_url = "https://www.iocl.com/Products/HighspeedDiesel.aspx"

PySQLPool.getNewPool().maxActiveConnections = 5
connection = PySQLPool.getNewConnection(username='root', password='password', host='localhost', db='petrol_prices')

app = Flask(__name__)


def ist_today():
    now = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
    today = datetime.datetime(now.year, now.month, now.day)
    return today


def get_prices_from_iocl_website():
    logging.error("fetching from iocl petrol website")
    petrol_page = requests.get(petrol_url)
    petrol_soup = BeautifulSoup(petrol_page.content, "html.parser")
    petrol_tables = petrol_soup.find_all('table', class_="product-table")

    logging.error("fetching from iocl diesel website")
    diesel_page = requests.get(diesel_url)
    diesel_soup = BeautifulSoup(diesel_page.content, "html.parser")
    diesel_tables = diesel_soup.find_all('table', class_="product-table")

    price_dict = {}

    for petrol_table in petrol_tables:
        for row in petrol_table.findAll("tr"):
            cells = row.findAll('td')
            city_name = str(cells[0].find(text=True)).strip()
            petrol_price = str(cells[1].find(text=True)).strip()
            price_dict.setdefault(city_name,{})["petrol"] = petrol_price

    for diesel_table in diesel_tables:
        for row in diesel_table.findAll("tr"):
            cells = row.findAll('td')
            city_name = str(cells[0].find(text=True)).strip()
            diesel_price = str(cells[1].find(text=True)).strip()
            price_dict.setdefault(city_name, {})["diesel"] = diesel_price

    date_divs = petrol_soup.find_all("div", class_="prod-table-top-note")
    petrol_date = list(datefinder.find_dates(date_divs[0].findAll("p")[0].get_text().strip()))[0].strftime('%s')#this is in utc
    round_time_to_day = roundTime(datetime.datetime.fromtimestamp(float(petrol_date),tz=pytz.timezone('Asia/Kolkata')), roundTo=24 * 60 * 60).strftime("%s") #this is in ist

    final_dict = {"status": {"message": "Successful", "code": 0},
                  "data": {"fuelprice": price_dict, "cities": price_dict.keys(), "timestamp": str(round_time_to_day)}}

    return final_dict


@app.route('/prices')
def prices():
    query = PySQLPool.getNewQuery(connection, commitOnEnd=True)
    query.Query("""select * from price_list where price_time = %s""", (ist_today(),))
    if int(query.rowcount) > 0:
        logging.error("found in db")
        price_dict = {}

        for record in query.record:
            if (record['city']) in price_dict:
                fuel_dict = price_dict[record['city']]
                fuel_dict[record['type']] = str(record['price'])
            else:
                fuel_dict = {}
                fuel_dict[record['type']] = str(record['price'])
                price_dict[record['city']] = fuel_dict

        prices_json = {"status": {"message": "Successful", "code": 0},
                       "data": {"fuelprice": price_dict, "cities": price_dict.keys()}}
    else:
        prices_json = get_prices_from_iocl_website()
        petrol_time = datetime.datetime.fromtimestamp(float(prices_json.get("data", {}).get("timestamp", ist_today())))
        for city, price_dict in prices_json.get("data", {}).get("fuelprice", {}).iteritems():
            for fuel_type, price in price_dict.iteritems():
                query.Query("""insert into price_list (city,price, type, price_time) values (%s,%s, %s, %s) on duplicate key update price=%s """,(city,price,fuel_type,petrol_time,price))

    return json.dumps(prices_json)

def roundTime(dt=None, roundTo=60):
    """Round a datetime object to any time laps in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
    """
    if dt == None: dt = datetime.datetime.now()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds + roundTo / 2) // roundTo * roundTo
    return dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)

import requests
import psycopg2
import psycopg2.extras
from datetime import datetime
import logging

def fetch_data():

    api_token='6f30855c6da0b95b'
    url='http://api.wunderground.com/api/'+api_token+'/conditions/q/CA/San_Francisco.json'
    r=requests.get(url).json()
    data=r['current_observation']

    location = data['observation_location']['full']
    weather = data['weather']
    wind_str = data['wind_string']
    temp = data['temp_f']
    humidity = data['relative_humidity']
    precip = data['precip_today_string']
    icon_url = data['icon_url']
    observation_time = data['observation_time']

    #open db
    try:
        conn = psycopg2.connect(dbname='weather', user='postgres',host='localhost',password='1234')
        print ('Opened DB Successfully')
    except:
        print (datetime.now(),'Unable to connect DB')
        logging.exception('Unable to opne DB')
        return
    else:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    #write data to DB
    cur.except("""INSERT INTO station_reading(location,weather,wind_str,temp,humidity,precip,icon_url,observation_time)
              VALUES(%s,%s,%s,%s,%s,%s,%s,%s)""",(location,weather,wind_str,temp,humidity,precip,
                                                 icon_url,observation_time))

    conn.commit()
    cur.close()
    conn.close()

    print("Data Written",datetime.now())

fetch_data()
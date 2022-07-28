import json
import os
import mysql.connector
from mysql.connector import Error
import requests
import pandas
from sqlalchemy import create_engine



# Define main script
from pandas import DataFrame
hostname=os.environ['hostname']
dbname=os.environ['dbname']
uname=os.environ['uname']
pwd=os.environ['pwd']



def main():
    import mysql.connector
    from mysql.connector import errorcode


    cnx = mysql.connector.connect(user='root',
                                      database='deals',
                                      host='localhost',
                                      password='061297')

    response = requests.get("https://www.cheapshark.com/api/1.0/deals?storeID=1&upperPrice=30")
    slug = (json.dumps(response.json(), indent=4, sort_keys=True))

    df = pandas.read_json(slug)
    df['lastChange'] = pandas.to_datetime(df['lastChange'], unit='s')
    df['releaseDate'] = pandas.to_datetime(df['releaseDate'], unit='s')




    engine = create_engine("mysql://{user}:{pw}@{host}/{db}"
                           .format(host=hostname, db=dbname, user=uname, pw=pwd))

    df.to_sql('daily_deals', engine, if_exists='append', index=False)

    print(df.to_string())
    cnx.close()

if __name__ == "__main__":
    main()

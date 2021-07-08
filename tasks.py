from celery import Celery
from clickhouse_driver import Client
from datetime import datetime
from time import sleep

import json
import requests

app = Celery('tasks', broker='amqp://guest:guest@localhost:5672/', backend='db+sqlite:///db.sqlite3')

@app.task
def calculate_points_sum():
    # connect to loyalty api
    url = 'https://test-task-loyalty-program-api.herokuapp.com/loyalty/client_balance/'
    client_balances = requests.get(url).json()

    # calculate sum of all points and run notifications
    points_sum = 0
    for balance in client_balances:
        if balance['points'] == 0:
            print(f"Client {balance['name']} with ID={balance['id']} has 0 points.") # change to email or messenger notification if needed
        elif balance['points'] >= 1_000:
            print(f"Client {balance['name']} with ID={balance['id']} has {balance['points']} points.") # change to email or messenger notification if needed

        points_sum += balance['points']

    if points_sum >= 100_000:
        print(f'Points sum in loyalty program is greater than 100 000 and equal to {points_sum}.') # change to email or messenger notification if needed

    print(points_sum)

    # connect and write to clickhouse database
    client = Client(host='localhost')

    client.execute('CREATE TABLE IF NOT EXISTS test (datetime DateTime, points Int32) ENGINE = Memory')
    client.execute('INSERT INTO test (datetime, points) VALUES (%(datetime)s, %(points_sum)s)', {'datetime': datetime.now(), 'points_sum': points_sum})

    # test print of all rows in default.test table to show code running correctly
    print(client.execute('SELECT * FROM test'))

if __name__ == "__main__":
    while(True):
        calculate_points_sum.delay()
        sleep(2)
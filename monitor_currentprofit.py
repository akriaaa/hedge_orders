from sched import scheduler
import pymysql
import datetime

import pandas as pd
import numpy as np

from binance import Client
import logging


class ShenDengDataBaseHedging:
    SQL = {
        'CUSTOMER_ORDERS_SELECT': """
        SELECT
            id,
            symbol_name,
            side,
            price,
            volume,
            price * volume AS turnover
        FROM
            hedge_filter_order 
        WHERE
            {condation}
            AND order_type in (1, 2, 3)
        """
    }
    
    def __init__(self):
        self.db_config = {
            'host': 'exchange-hedge.cmvznimubfkb.ap-southeast-1.rds.amazonaws.com',
            'user': 'ex_lijingang',
            'password': 'ow98al0eq64ex9qnmmnx30xwn0tkmvgk',
            'db': 'exchange_hedge',
            'port': 3306
        }
                
    def start_time(self) -> str:
        return datetime.datetime.now().strftime('%Y-%m-%d 00:00:00')
    
    def new_time(self) -> str:
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def end_time(self) -> str:
        return datetime.datetime.now().strftime('%Y-%m-%d')
    
    def execute_sql(self, sql: str) -> pd.DataFrame:
        with pymysql.connect(**self.db_config) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
    
    def update_date(self, start_time: str, end_time: str):
        self.customer_data = f'bill_time between "{start_time}" and "{end_time} 23:59:59"'


    def new_customer_orders(self) -> pd.DataFrame:
        self.update_date(start_time=self.start_time(), end_time=self.end_time())
        sql = self.SQL['CUSTOMER_ORDERS_SELECT'].format(condation=self.customer_data)
        result = self.execute_sql(sql)
        return pd.DataFrame(result, columns=['id', 'symbol', 'side', 'orderprice', 'volume', 'turnover'])
    
    def price(self) ->  pd.DataFrame:
        client = Client()
        ticker = client.get_symbol_ticker()
        return pd.DataFrame(ticker, columns=['symbol', 'price'])
    
    def data(self) -> pd.DataFrame:
        customer_orders = self.new_customer_orders()
        price = self.price()
        data = pd.merge(customer_orders, price, on='symbol', how='left')
        data['side'] = data['side'].apply(lambda x: 'buy' if x == 1 else 'sell')
        data[['price', 'orderprice']] = data[['price', 'orderprice']].astype(float)

        data['price'] = np.where(data['price'].isnull(), data['orderprice'], data['price'])
        data['profit'] = np.where(data['side'] == 'buy', data['orderprice'] - data['price'], data['price'] - data['orderprice'])

        profit = data['profit'].sum()
        logging.info(f'time: {self.new_time()} profit: {profit}')
        return data
    

    def main(self):
        aps = scheduler.schedulers.blocking.BlockingScheduler()
        aps.add_job(self.data, 'cron', hour='/*1')
        aps.start()
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ShenDengDataBaseHedging().data()

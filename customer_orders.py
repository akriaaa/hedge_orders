import pymysql
import datetime
import schedule
import time

import pandas as pd
import numpy as np
from typing import Dict, Sequence, Union, Set

from binance import Client
from matplotlib import pyplot as plt


class ShenDengDataBaseHedging:
    class SQL:
        CUSTOMER_ORDERS_SELECT = """
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
        
    def __init__(self):
        self.host = 'exchange-hedge.cmvznimubfkb.ap-southeast-1.rds.amazonaws.com'
        self.user = 'ex_lijingang'
        self.password = 'ow98al0eq64ex9qnmmnx30xwn0tkmvgk'
        self.db = 'exchange_hedge'
        self.port = 3306
        
        # self.user = 'QuantitativeTrading'
        # self.password = '7qo6g3vo0licd09kbmm9x6k3l4bbve'
        # self.db = 'exchange_hedge'
        # self.port = 3306
        
    def start_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d 00:00:00')
    
    def new_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def end_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d')
    
    
    def execute_sql(self, sql) -> pd.DataFrame:
        conn = pymysql.connect(host=self.host, user=self.user, password=self.password, db=self.db, port=self.port)
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    
    def update_date(self, start_time, end_time):
        self.customer_data = f'bill_time between "{start_time}" and "{end_time} 23:59:59"'


    def main(self):
        schedule.every(1).minutes.do(self.run)
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    def run(self):
        try:
            self.data()
        except Exception as e:
            print(e)
            time.sleep(5)
            self.main()


    def new_customer_orders(self) -> pd.DataFrame:
        self.update_date(start_time=self.start_time(), end_time=self.end_time())
        sql = self.SQL.CUSTOMER_ORDERS_SELECT.format(condation = self.customer_data)
        result = self.execute_sql(sql)
        result = pd.DataFrame(result, columns=['id', 'symbol', 'side', 'orderprice', 'volume', 'turnover'])

        return result
    
    def price(self) ->  pd.DataFrame:
        client = Client()
        ticker = client.get_symbol_ticker()
        result = pd.DataFrame(ticker, columns=['symbol', 'price'])
        return result
    
    def data(self) -> pd.DataFrame:
        customer_orders = self.new_customer_orders()
        price = self.price()
        data = pd.merge(customer_orders, price, on='symbol', how='left')
        data['side'] = data['side'].apply(lambda x: 'buy' if x == 1 else 'sell')
        data['price'] = data['price'].astype(float)
        data['orderprice'] = data['orderprice'].astype(float)

        data['price'] = np.where(data['price'].isnull(), data['orderprice'], data['price'])
        data['profit'] = np.where(data['side'] == 'buy', data['orderprice'] - data['price'], data['price'] - data['orderprice'])

        profit = data['profit'].sum()
        print(f'当前时间: {self.new_time()}', f'当前盈利: {profit}')
        return data
    
    def plot_current_profit(self):
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        profit = self.data()['profit'].sum()
        
        plt.bar(current_time, profit)
        plt.xlabel('Time')
        plt.ylabel('Profit')
        plt.title('Current Profit')
        plt.xticks(rotation=45)
        plt.grid()
        plt.show()


if __name__ == '__main__':
    shendeng = ShenDengDataBaseHedging()
    shendeng.main()
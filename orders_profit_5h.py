import calendar
import datetime
import ccxt
import pandas as pd
import pymysql
from datetime import timedelta
import concurrent.futures
from typing import Dict

import pytz

class AmplitudeAnalyzer:
    db_config = {
        "host": "exchange-hedge.cmvznimubfkb.ap-southeast-1.rds.amazonaws.com",
        "user": "ex_lijingang",
        "password": "ow98al0eq64ex9qnmmnx30xwn0tkmvgk",
        "database": "exchange_hedge",
        "port": 3306
    }
    # db_config = {
    #     "host": "exchange-hedge.cmvznimubfkb.ap-southeast-1.rds.amazonaws.com",
    #     "user": "QuantitativeTrading",
    #     "password": "7qo6g3vo0licd09kbmm9x6k3l4bbve",
    #     "database": "exchange_hedge",
    #     "port": 3306
    # }

    def __init__(self):
        self.exchange = ccxt.bitrue()

    def get_user_orders(self) -> pd.DataFrame:
        connection = pymysql.connect(**self.db_config)
        try:
            with connection.cursor() as cursor:
                sql = """
                SELECT
                id,
                symbol_name,
                price,
                volume,
                side,
                UNIX_TIMESTAMP(bill_time) * 1000 as bill_time_ms
                FROM
                    hedge_filter_order
                WHERE 
                    bill_time BETWEEN '2023-03-25' AND '2023-03-25 23:59:59'
                    AND symbol_name in ('ETHUSDT', 'BTCUSDT', 'XRPUSDT')
                    AND order_type in (1, 2, 3)
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                results = pd.DataFrame(results, columns=["id", "symbol_name", "price", "volume", "side", "bill_time"])
                print(results)
        finally:
            connection.close()
        return results

    
    def get_klines(self, symbol, start_time, end_time, interval="1h"):
        timeframe = interval
        klines = []

        since = start_time

        while since < end_time:
            try:
                new_klines = self.exchange.fetch_ohlcv(symbol, timeframe, since)
                if not new_klines:
                    break
                klines += new_klines
                since = klines[-1][0] + self.exchange.parse_timeframe(timeframe) * 1000
            except Exception as e:
                print(e)
                break

        klines = [kline for kline in klines if start_time <= kline[0] < end_time]
        return klines



    def calculate_high_low(self, klines):
        if not klines:
            return None, None
        high = max([float(kline[2]) for kline in klines])
        low = min([float(kline[3]) for kline in klines])
        return high, low


    # def analyze_orders(self, user_orders) -> pd.DataFrame:
    #     order_analysis_results = []

    #     with concurrent.futures.ThreadPoolExecutor() as executor:
    #         futures = [executor.submit(self.analyze_single_order, order) for _, order in user_orders.iterrows()]

    #         for future in concurrent.futures.as_completed(futures):
    #             try:
    #                 order_analysis = future.result()
    #                 order_analysis_results.append(order_analysis)
    #             except Exception as e:
    #                 print(f"Analyze order failed: {e}")

    #     results_df = pd.DataFrame(order_analysis_results)
    #     results_df = results_df.astype({'price': 'float', 'volume': 'float', 'high': 'float', 'low': 'float'})
    #     print(results_df)
    #     return results_df

    # def analyze_single_order(self, order: pd.Series) -> Dict:
    #     id, symbol_name, price, volume, side, bill_time = order
    #     symbol = symbol_name.upper()
    #     order_time = bill_time
    #     start_time = int(order_time.timestamp() * 1000)
    #     end_time = int((order_time + timedelta(hours=5)).timestamp() * 1000)

    #     klines = self.get_klines(symbol, start_time, end_time)
    #     high, low = self.calculate_high_low(klines)

    #     order_analysis = {
    #         "id": id,
    #         "symbol": symbol,
    #         "price": price,
    #         "side": side,
    #         "volume": volume,
    #         "high": high,
    #         "low": low
    #     }
    #     return order_analysis


    def analyze_orders(self, user_orders):

        order_analysis_results = []
        for id, symbol_name, price, volume, side, bill_time in user_orders.values:
            symbol = symbol_name.upper()
            start_time = bill_time
            end_time = bill_time + 5 * 60 * 60 * 1000

            klines = self.get_klines(symbol, start_time, end_time)
            high, low = self.calculate_high_low(klines)
            #print(f"订单 {id} 在 {order_time} 后的五个小时内最高点：{high}, 最低点：{low}")
            order_analysis_results.append({
                "id": id,
                "symbol": symbol,
                "price": price,
                "side": side,
                "volume": volume,
                "high": high,
                "low": low
            })
            
        results_df = pd.DataFrame(order_analysis_results)
        results_df = results_df.astype({'price': 'float', 'volume': 'float', 'high': 'float', 'low': 'float'})
        return results_df
        

    def calculate_profit_loss(self, results_df):
        profit_loss = []

        for index, row in results_df.iterrows():
            if row['side'] == 1:
                max_loss = (row['high'] - row['price']) * row['volume']
                max_profit = (row['price'] - row['low']) * row['volume']
            elif row['side'] == 2:
                max_loss = (row['price'] - row['low']) * row['volume']
                max_profit = (row['high'] - row['price']) * row['volume']

            profit_loss.append({
                "max_profit": max_profit,
                "max_loss": max_loss
            })

        profit_loss_df = pd.DataFrame(profit_loss)
        results_with_profit_loss = pd.concat([results_df, profit_loss_df], axis=1)
        return results_with_profit_loss


    def main(self):
        try:
            user_orders = self.get_user_orders()
            results_df = self.analyze_orders(user_orders)
            results_with_profit_loss = self.calculate_profit_loss(results_df)
            print(results_with_profit_loss)
            results_with_profit_loss.to_csv("orders_profit_5h.csv", index=False)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    analyzer = AmplitudeAnalyzer()
    analyzer.main()

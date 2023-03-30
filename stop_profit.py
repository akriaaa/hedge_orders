import datetime
import pandas as pd
import pytz
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pymysql

class AmplitudeAnalyzer:
    # db_config = {
    #     "host": "exchange-hedge.cmvznimubfkb.ap-southeast-1.rds.amazonaws.com",
    #     "user": "ex_lijingang",
    #     "password": "ow98al0eq64ex9qnmmnx30xwn0tkmvgk",
    #     "database": "exchange_hedge",
    #     "port": 3306
    # }
    db_config = {
        "host": "exchange-hedge.cmvznimubfkb.ap-southeast-1.rds.amazonaws.com",
        "user": "QuantitativeTrading",
        "password": "7qo6g3vo0licd09kbmm9x6k3l4bbve",
        "database": "exchange_hedge",
        "port": 3306    
    }

    def __init__(self):
        self.client = Client()

    def get_large_orders(self) -> pd.DataFrame:
        connection = pymysql.connect(**self.db_config)
        try:
            with connection.cursor() as cursor:
                sql = """
                SELECT
                symbol_name,
                base_coin,
                quote_coin,
                ABS(SUM(CASE WHEN side = 1 THEN volume ELSE 0 END) - SUM(CASE WHEN side = 2 THEN volume ELSE 0 END)) AS volume,
                ABS(SUM(CASE WHEN side = 1 THEN price * volume ELSE 0 END) - SUM(CASE WHEN side = 2 THEN price * volume ELSE 0 END)) AS turnover,
                CASE 
                    WHEN SUM(CASE WHEN side = 1 THEN price * volume ELSE 0 END) - SUM(CASE WHEN side = 2 THEN price * volume ELSE 0 END) > 0 THEN 1
                    ELSE 2
                END AS side 
                FROM
                    hedge_filter_order
                WHERE 
                    bill_time > '2023-03-25'
                GROUP BY
                    symbol_name
                """
                cursor.execute(sql)
                results = cursor.fetchall()
                results = pd.DataFrame(results, columns=["symbol_name", "base_coin", "quote_coin", "volume", "turnover", "side"])
        finally:
            connection.close()
        return results

    def get_amplitude(self, symbol, interval, start_time, end_time):
        try:
            klines = self.client.get_klines(symbol=symbol, interval=interval, startTime=start_time, endTime=end_time)
            if not klines:
                return None
            high_prices = [float(k[2]) for k in klines]
            low_prices = [float(k[3]) for k in klines]
            amplitude = (max(high_prices) - min(low_prices)) / min(low_prices)
        except BinanceAPIException as e:
            print(f"Binance API error: {e}")
            return None
        return amplitude

    def analyze(self):
        orders = self.get_large_orders()
        utc_now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        one_hour_ago = utc_now - datetime.timedelta(hours=1)
        timestamp_one_hour_ago = int(one_hour_ago.timestamp() * 1000)

        for symbol_name, base_coin, quote_coin, volume, turnover, side in orders.values:
            amplitude = self.get_amplitude(symbol_name, Client.KLINE_INTERVAL_1HOUR, timestamp_one_hour_ago, None)
            print(f"{symbol_name} - {amplitude}")
            with open("amplitude.txt", "a") as f:
                f.write(f"{symbol_name} {amplitude}\n")


if __name__ == "__main__":
    analyzer = AmplitudeAnalyzer()
    analyzer.analyze()

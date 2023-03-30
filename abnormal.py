

from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
import pymysql
import telepot

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

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
        pass

    def send_telegram_message(self, message):
        bot = telepot.Bot("5689177629:AAFrR0LdR134Rzn5F5T4cd0fRz_g7m4Z0Fo")
        chat_id = "808400546"

        bot.sendMessage(chat_id, message)


    def get_user_orders(self) -> pd.DataFrame:
        connection = pymysql.connect(**self.db_config)
        try:
            with connection.cursor() as cursor:
                sql = """
                SELECT 
                    dv.symbol_name, 
                    dv.total_volume, 
                    sdav.avg_volume, 
                    dv.total_volume / sdav.avg_volume AS volume_ratio
                FROM 
                    (SELECT 
                        symbol_name, 
                        SUM(price * volume) AS total_volume, 
                        DATE(bill_time) AS bill_date
                    FROM 
                        hedge_filter_order
                    WHERE 
                        bill_time >= CURRENT_DATE 
                        AND order_type in (1, 2, 3)
                    GROUP BY 
                        symbol_name, bill_date
                    HAVING 
                        total_volume > 50000
                        AND bill_date = CURRENT_DATE) dv
                    JOIN 
                        (SELECT 
                            symbol_name,
                            AVG(total_volume) AS avg_volume
                        FROM 
                            (SELECT 
                                symbol_name, 
                                SUM(price * volume) AS total_volume, 
                                DATE(bill_time) AS bill_date
                            FROM 
                                hedge_filter_order
                            WHERE 
                                bill_time >= CURRENT_DATE - INTERVAL 7 DAY 
                                AND bill_time < CURRENT_DATE
                                AND order_type in (1, 2, 3)
                            GROUP BY 
                                symbol_name, bill_date
                            HAVING 
                                total_volume > 50000) daily_volume
                        GROUP BY 
                            symbol_name) sdav 
                    ON dv.symbol_name = sdav.symbol_name
                WHERE 
                    dv.total_volume / sdav.avg_volume > 100
                ORDER BY 
                    volume_ratio DESC;

                """
                cursor.execute(sql)
                results = cursor.fetchall()
                results = pd.DataFrame(results, columns=["symbol", "total_volume", "avg_volume","volume_ratio"])
                print(results)
        finally:
            connection.close()
        return results
    
    def send_message(self):
        results = self.get_user_orders()
        if not results.empty:
            message = "以下交易对的交易量比率大于1：\n\n"
            for result in results.values:
                message += f"{result[0]}: {result[1]:.2f}, {result[2]:.2f}, {result[3]:.2f}\n"
            self.send_telegram_message(message)


    def run(self):
        scheduler = BlockingScheduler()
        scheduler.add_job(self.send_message, 'cron', minute='*/1')
        scheduler.start()
        

    
if __name__ == "__main__":
    analyzer = AmplitudeAnalyzer()
    analyzer.run()
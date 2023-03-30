from sched import scheduler
import ccxt
import numpy as np
import time

class TrendStrategy:
    def __init__(self, symbols=None, timeframe='15m', limit=56):
        self.bitrue = ccxt.bitrue()
        self.symbols = symbols or ['BTC/USDT']
        self.timeframe = timeframe
        self.limit = limit

    @staticmethod
    def sma(data, window):
        return np.convolve(data, np.ones(window), 'valid') / window

    def fetch_data(self, symbol):
        candles = self.bitrue.fetch_ohlcv(symbol, self.timeframe, limit=self.limit)
        close_prices = [candle[4] for candle in candles]
        return close_prices

    def execute_strategy(self):
        for symbol in self.symbols:
            close_prices = self.fetch_data(symbol)
            sma_20 = self.sma(np.array(close_prices), 20)
            sma_55 = self.sma(np.array(close_prices), 55)

            if len(sma_20) > 0 and len(sma_55) > 0:
                last_close = close_prices[-1]
                last_sma_20 = sma_20[-1]
                last_sma_55 = sma_55[-1]

                print("Symbol:", symbol)
                print("Last Close:", last_close)
                print("Last SMA 20:", last_sma_20)
                print("Last SMA 55:", last_sma_55)

                if last_close > last_sma_20 > last_sma_55:
                    print("上涨趋势")
                    # 保留空单，平仓多单操作
                    # ...
                elif last_close < last_sma_20 < last_sma_55:
                    print("下跌趋势")
                    # 保留多单，平仓空单操作
                    # ...
                else:
                    print("震荡")
                    # 不做任何操作
                    # ...

    def main(self):
        try:
            self.execute_strategy()
        except Exception as e:
            print(e)
            time.sleep(5)
            self.main()

    def run(self):
        aps = scheduler.schedulers.background.BackgroundScheduler()
        aps.add_job(self.main, 'cron', second='*/15')
        aps.start()

if __name__ == "__main__":
    symbols = ['BTC/USDT', 'ETH/USDT', 'XRP/USDT']
    strategy = TrendStrategy(symbols=symbols)
    strategy.execute_strategy()

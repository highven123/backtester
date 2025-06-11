import backtrader as bt

class BaseStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def __init__(self):
        self.trades_log = []
        self.trades = []
        self.current_trade = None

    def store_trade(self, type, size, price):
        self.current_trade = {
            'type': type,
            'date': self.data.datetime.date(),
            'price': price,
            'size': size,
            'value': size * price
        }

    def close_trade(self, price):
        if self.current_trade:
            self.current_trade.update({
                'sell_date': self.data.datetime.date(),
                'sell_price': price,
                'profit': (price - self.current_trade['price']) * self.current_trade['size']
            })
            self.trades.append(self.current_trade)
            self.current_trade = None

class MovingAverageStrategy(BaseStrategy):
    params = (('short_window', 20), ('long_window', 50), ('position_size', 0.1))

    def __init__(self):
        super().__init__()
        self.short_ma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.short_window)
        self.long_ma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.long_window)
        self.crossover = bt.indicators.CrossOver(self.short_ma, self.long_ma)

    def next(self):
        if not self.position:
            if self.crossover > 0:
                size = self.broker.getvalue() * self.params.position_size / self.data.close[0]
                self.buy(size=size)
                self.store_trade('buy', size, self.data.close[0])
        else:
            if self.crossover < 0:
                self.close()
                self.close_trade(self.data.close[0])

class RSIStrategy(BaseStrategy):
    params = (('rsi_period', 14), ('rsi_entry_oversold', 30), ('rsi_exit_oversold', 45), ('rsi_entry_overbought', 70), ('position_size', 0.1))

    def __init__(self):
        super().__init__()
        self.rsi = bt.indicators.RSI(self.data.close, period=self.params.rsi_period)

    def next(self):
        if not self.position:
            if self.rsi < self.params.rsi_entry_oversold:
                size = self.broker.getvalue() * self.params.position_size / self.data.close[0]
                self.buy(size=size)
                self.store_trade('buy', size, self.data.close[0])
        else:
            if self.rsi > self.params.rsi_entry_overbought or self.rsi > self.params.rsi_exit_oversold:
                self.close()
                self.close_trade(self.data.close[0])

class MACDStrategy(BaseStrategy):
    params = (('macd_fast', 12), ('macd_slow', 26), ('macd_signal', 9), ('position_size', 0.1))

    def __init__(self):
        super().__init__()
        self.macd = bt.indicators.MACD(self.data.close, period_me1=self.params.macd_fast, period_me2=self.params.macd_slow, period_signal=self.params.macd_signal)

    def next(self):
        if not self.position:
            if self.macd.macd > self.macd.signal:
                size = self.broker.getvalue() * self.params.position_size / self.data.close[0]
                self.buy(size=size)
                self.store_trade('buy', size, self.data.close[0])
        else:
            if self.macd.macd < self.macd.signal:
                self.close()
                self.close_trade(self.data.close[0])

import yfinance as yf
import pandas as pd
import akshare as ak
import ccxt
import requests

def fetch_stock_data(symbol, start_date, end_date):
    df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''))
    if df is None or df.empty:
        return None
    df.rename(columns={'开盘': 'Open', '收盘': 'Close', '最高': 'High', '最低': 'Low', '成交量': 'Volume', '日期': 'Date'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    return df[['Open', 'High', 'Low', 'Close', 'Volume']]

def fetch_crypto_data(symbol, start_date, end_date):
    exchange = ccxt.binance()
    since = int(pd.to_datetime(start_date).timestamp() * 1000)
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', since=since, limit=1000)
    if not ohlcv:
        return None
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['Date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('Date', inplace=True)
    df = df[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
    return df[['Open', 'High', 'Low', 'Close', 'Volume']]

def fetch_forex_data(symbol, start_date, end_date, data_source='yfinance', alpha_vantage_key=None):
    if data_source == 'yfinance':
        df = yf.download(symbol, start=start_date, end=end_date)
        if df.empty:
            return None
        df = df.rename(columns={'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close', 'Volume': 'Volume'})
        df.index.name = 'Date'
        return df[['Open', 'High', 'Low', 'Close']]
    elif data_source == 'alphavantage':
        if not alpha_vantage_key:
            raise ValueError("需要提供 Alpha Vantage API KEY")
        base, quote = symbol[:3], symbol[3:6]
        url = f"https://www.alphavantage.co/query?function=FX_DAILY&from_symbol={base}&to_symbol={quote}&outputsize=full&apikey={alpha_vantage_key}"
        r = requests.get(url)
        data = r.json().get('Time Series FX (Daily)', {})
        if not data:
            return None
        df = pd.DataFrame(data).T.astype(float)
        df.index = pd.to_datetime(df.index)
        df.columns = ['Open', 'High', 'Low', 'Close']
        df = df.sort_index()
        df = df[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]
        return df
    else:
        raise ValueError("暂不支持的数据源")

def get_data(market, symbol, start_date, end_date, fx_data_source='yfinance', alpha_vantage_key=None):
    if market == '沪深A股':
        return fetch_stock_data(symbol, start_date, end_date)
    elif market == '加密货币':
        return fetch_crypto_data(symbol, start_date, end_date)
    elif market == '外汇':
        return fetch_forex_data(symbol, start_date, end_date, data_source=fx_data_source, alpha_vantage_key=alpha_vantage_key)
    else:
        raise ValueError("暂不支持的市场类型")

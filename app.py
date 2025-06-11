import streamlit as st
from datetime import datetime, timedelta
import pandas as pd

from data_handler import get_data
from strategy_handler import MovingAverageStrategy, RSIStrategy, MACDStrategy
from report_utils import display_backtest_results

import backtrader as bt

def run_backtest(data, strategy_class, strategy_params):
    if data is None or data.empty:
        st.error("没有有效的数据用于回测")
        return None
    cerebro = bt.Cerebro()
    data_feed = bt.feeds.PandasData(dataname=data, datetime=None, open='Open', high='High', low='Low', close='Close', volume='Volume', openinterest=-1)
    cerebro.adddata(data_feed)
    cerebro.broker.setcash(strategy_params.get('initial_cash', 10000))
    cerebro.broker.setcommission(commission=0.001)
    cerebro.addstrategy(strategy_class, **{k: v for k, v in strategy_params.items() if k != 'initial_cash'})
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
    results = cerebro.run()
    if not results:
        return None
    strategy = results[0]
    final_value = cerebro.broker.getvalue()
    total_return = ((final_value - strategy_params.get('initial_cash', 10000)) / strategy_params.get('initial_cash', 10000) * 100)
    sharpe_ratio = strategy.analyzers.sharpe.get_analysis().get('sharperatio', 0)
    max_drawdown = strategy.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0)
    trades_analysis = strategy.analyzers.trades.get_analysis()
    total_trades = trades_analysis.get('total', {}).get('total', 0)
    total_won = trades_analysis.get('won', {}).get('total', 0)
    win_rate = (total_won / total_trades * 100) if total_trades > 0 else 0.0
    equity_curve = pd.Series([strategy_params.get('initial_cash', 10000), final_value], index=[data.index[0], data.index[-1]])
    return {
        'total_return': total_return,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown': max_drawdown,
        'win_rate': win_rate,
        'total_trades': total_trades,
        'total_won': total_won,
        'equity_curve': equity_curve,
        'trades': getattr(strategy, 'trades', []),
    }

def main():
    st.title("📈 多市场策略回测系统（含外汇）")

    market = st.sidebar.selectbox("市场", ["沪深A股", "加密货币", "外汇"])
    symbol = st.sidebar.text_input("标的代码", value="600519" if market == "沪深A股" else "BTC/USDT" if market == "加密货币" else "EURUSD=X")
    start_date = st.sidebar.date_input("开始日期", value=datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date = st.sidebar.date_input("结束日期", value=datetime.now()).strftime("%Y-%m-%d")
    strategy_name = st.sidebar.selectbox("策略", ["双均线", "RSI", "MACD"])
    st.sidebar.markdown("---")
    st.sidebar.markdown("**策略参数**")
    if strategy_name == "双均线":
        short_window = st.sidebar.number_input("短期均线", 5, 50, 20)
        long_window = st.sidebar.number_input("长期均线", 10, 200, 50)
        position_size = st.sidebar.number_input("仓位", 0.01, 1.0, 0.1)
        strategy_class = MovingAverageStrategy
        strategy_params = {'short_window': short_window, 'long_window': long_window, 'position_size': position_size}
    elif strategy_name == "RSI":
        rsi_period = st.sidebar.number_input("RSI周期", 5, 50, 14)
        rsi_entry_oversold = st.sidebar.number_input("超卖买入阈值", 10, 40, 30)
        rsi_exit_oversold = st.sidebar.number_input("止盈阈值", 30, 60, 45)
        rsi_entry_overbought = st.sidebar.number_input("超买卖出阈值", 60, 90, 70)
        position_size = st.sidebar.number_input("仓位", 0.01, 1.0, 0.1)
        strategy_class = RSIStrategy
        strategy_params = {
            'rsi_period': rsi_period,
            'rsi_entry_oversold': rsi_entry_oversold,
            'rsi_exit_oversold': rsi_exit_oversold,
            'rsi_entry_overbought': rsi_entry_overbought,
            'position_size': position_size
        }
    else:  # MACD
        macd_fast = st.sidebar.number_input("MACD快线", 5, 30, 12)
        macd_slow = st.sidebar.number_input("MACD慢线", 10, 50, 26)
        macd_signal = st.sidebar.number_input("信号周期", 5, 20, 9)
        position_size = st.sidebar.number_input("仓位", 0.01, 1.0, 0.1)
        strategy_class = MACDStrategy
        strategy_params = {
            'macd_fast': macd_fast, 'macd_slow': macd_slow, 'macd_signal': macd_signal, 'position_size': position_size
        }
    strategy_params['initial_cash'] = st.sidebar.number_input("初始资金", 1000, 1000000, 10000, step=1000)
    if st.sidebar.button("执行回测"):
        with st.spinner("正在获取数据..."):
            data = get_data(market, symbol, start_date, end_date)
            if data is None or len(data) == 0:
                st.error("数据获取失败，请检查代码和时间区间！")
                return
            st.success("数据获取成功，正在回测…")
            results = run_backtest(data, strategy_class, strategy_params)
            display_backtest_results(results)

if __name__ == "__main__":
    main()

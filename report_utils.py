import streamlit as st
import pandas as pd
import plotly.graph_objects as go

def display_backtest_results(results):
    if results is None:
        st.error("没有回测结果可供显示")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("总收益率", f"{results['total_return']:.2f}%")
    col1.metric("总盈亏金额", f"${results.get('total_profit', 0):.2f}")
    col2.metric("夏普比率", f"{results['sharpe_ratio']:.2f}")
    col2.metric("总交易次数", str(results['total_trades']))
    col3.metric("最大回撤", f"{results['max_drawdown']:.2f}%")
    col3.metric("胜率", f"{results['win_rate']:.2f}%")

    equity_curve = results.get('equity_curve')
    if equity_curve is not None and not equity_curve.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=equity_curve.index, y=equity_curve.values, mode='lines', name='权益曲线'))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("没有权益曲线数据可供显示")

    trades = results.get('trades', [])
    if trades:
        st.subheader("交易记录")
        trades_df = pd.DataFrame(trades)
        st.dataframe(trades_df)
    else:
        st.warning("回测期间没有产生任何交易")

from datetime import datetime
import akshare as ak
import numpy as np
import pandas as pd
import backtrader as bt

class MA_Cross(bt.Strategy):
    params = (
        ('pfast', 5),  # period for the fast moving average
        ('pslow', 10),   # period for the slow moving average
    )

    def log(self, txt, dt=None):
        """Logging function fot this strategy"""
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.volume = self.datas[0].volume
        self.order = None
        self.buyprice = None
        self.buycomm = None

        sma1 = bt.ind.SMA(period=self.params.pfast)  # fast moving average
        sma2 = bt.ind.SMA(period=self.params.pslow)  # slow moving average
        self.crossover = bt.ind.CrossOver(sma1, sma2)  # crossover signal

    def next(self):
        self.log("Close, %.2f" % self.dataclose[0])

        # 检查是否有未完成订单
        if self.order:
            return
            
        # 未持有仓位
        if not self.position:
            if self.crossover > 0:  # 金叉
                self.log("BUY CREATE, %.2f" % self.dataclose[0])
                # 计算购买数量：95%现金除以当前价格
                cash = self.broker.getcash()
                size = int(cash * 0.95 / self.dataclose[0])
                self.buy(size=size)
        # 已持有仓位
        else:
            if self.crossover < 0:  # 死叉
                self.log("SELL CREATE, %.2f" % self.dataclose[0])
                self.sell(size=self.position.size)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
            
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    "BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f"
                    % (order.executed.price, order.executed.value, order.executed.comm)
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(
                    "SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f"
                    % (order.executed.price, order.executed.value, order.executed.comm)
                )
                
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Canceled/Margin/Rejected")
            
        # 重置订单状态
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
            
        self.log("OPERATION PROFIT, GROSS %.2f, NET %.2f" % (trade.pnl, trade.pnlcomm))

def get_data(code, start="2020-01-01", end="2025-11-30"):
    """获取并处理股票数据"""
    # 获取股票历史数据
    df = ak.stock_zh_a_hist(
        symbol=code, 
        period="daily", 
        start_date=start.replace("-", ""), 
        end_date=end.replace("-", ""), 
        adjust="qfq"
    )
    
    # 重命名列以匹配Backtrader要求
    df.rename(columns={
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume"
    }, inplace=True)
    
    # 转换日期列为datetime类型并设置为索引
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    
    # 添加openinterest列（Backtrader要求）
    df["openinterest"] = 0
    
    # 筛选需要的列
    df = df[["open", "high", "low", "close", "volume", "openinterest"]]
    
    return df

if __name__ == "__main__":
    # 设置回测参数
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 11, 30)
    stock_code = "000625"  # 长安汽车/
    
    # 获取并处理数据
    dataframe = get_data(stock_code, start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"))
    
    # 创建Data Feed
    data = bt.feeds.PandasData(
        dataname=dataframe,
        datetime=None,  # 使用索引作为日期
        open=0, high=1, low=2, close=3, volume=4, openinterest=5
    )
    
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(MA_Cross)
    
    # 添加数据
    cerebro.adddata(data)
    
    # 设置初始资金
    cerebro.broker.setcash(10000)
    
    # 设置佣金
    cerebro.broker.setcommission(commission=0.0005)
    
    print("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())
    
    # 运行策略
    cerebro.run()
    
    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())
    
    # 设置收盘价线样式 - 正确的方法
    # 获取数据对象
    data = cerebro.datas[0]
    
    # 设置收盘价线的样式
    data.plotinfo.plotlines = dict(
        close=dict(
            color='lightgray',  # 浅灰色
            linewidth=2.0,     # 加粗线宽
            linestyle='-',      # 实线
        )
    )
    
    # 绘制图表
    cerebro.plot()
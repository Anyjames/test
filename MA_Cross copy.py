from datetime import datetime
import numpy as np
import pandas as pd
import backtrader as bt
import akshare as ak

class MACDStrategy(bt.Strategy):
    params = (
        ('fast', 12),   # MACD快线周期
        ('slow', 26),   # MACD慢线周期
        ('signal', 9),  # MACD信号线周期
    )

    def log(self, txt, dt=None):
        """日志记录函数"""
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.buyprice = None
        self.buycomm = None
        
        # 计算MACD指标 - 使用正确的Backtrader MACD实现
        self.macd = bt.indicators.MACD(
            self.data.close,
            period_me1=self.params.fast,
            period_me2=self.params.slow,
            period_signal=self.params.signal
        )
        
        # 计算MACD交叉信号
        self.crossover = bt.indicators.CrossOver(self.macd.macd, self.macd.signal)
        
        # 添加其他指标用于分析
        self.sma_fast = bt.indicators.SMA(self.data.close, period=5)
        self.sma_slow = bt.indicators.SMA(self.data.close, period=10)
        
        print(f"策略初始化完成 - MACD参数: {self.params.fast}, {self.params.slow}, {self.params.signal}")

    def next(self):
        # 记录当前价格和指标值
        self.log("Close: %.2f, MACD: %.2f, Signal: %.2f" % (
            self.dataclose[0],
            self.macd.macd[0],
            self.macd.signal[0]
        ))

        # 检查是否有未完成订单
        if self.order:
            return
            
        # 未持有仓位
        if not self.position:
            if self.crossover > 0:  # MACD线上穿信号线
                # 计算购买数量：95%现金除以当前价格
                cash = self.broker.getcash()
                size = int(cash * 0.95 / self.dataclose[0])
                if size > 0:
                    self.log(f"BUY CREATE, Size: {size}, Price: {self.dataclose[0]:.2f}")
                    self.order = self.buy(size=size)
        # 已持有仓位
        else:
            if self.crossover < 0:  # MACD线下穿信号线
                self.log(f"SELL CREATE, Price: {self.dataclose[0]:.2f}")
                self.order = self.sell(size=self.position.size)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
            
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    "BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %d" %
                    (order.executed.price, order.executed.value, 
                     order.executed.comm, order.executed.size)
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(
                    "SELL EXECUTED, Price: %.极f, Cost: %.2f, Comm %.2f, Size: %d" %
                    (order.executed.price, order.executed.value, 
                     order.executed.comm, order.exec极ted.size)
                )
                
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Canceled/Margin/Rejected")
            
        # 重置订单状态
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
            
        self.log("OPERATION PROFIT, GROSS %.2f, NET %.2f" % (trade.pnl, trade.pnlcomm))

    def stop(self):
        # 策略结束时打印最终结果
        self.log("最终组合价值: %.2f" % self.broker.getvalue())
        print('MACD参数: (%2d, %2d, %2d) 最终价值: %.2f' %
              (self.params.fast, self.params.slow, self.params.signal, self.broker.getvalue()))

def get_data(code, start="2022-01-01", end="2024-09-30"):
    """获取基金数据"""
    print(f"尝试获取基金 {code} 数据...")
    
    try:
        # 使用正确的AKShare接口获取基金数据
        # 根据AKShare文档，使用fund_em_open_fund_daily接口
        df = ak.fund_em_open_fund_daily(symbol=code)
        print(f"成功获取基金 {code} 数据，共 {len(df)} 条记录")
        
        # 处理数据
        df.rename(columns={
            '净值日期': 'date',
            '单位净值': 'close'
        }, inplace=True)
        
        # 设置日期索引
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
    except Exception as e:
        print(f"获取基金数据失败: {e}")
        print("使用模拟数据作为回退")
        
        # 创建模拟数据
        dates = pd.date_range(start=start, end=end)
        prices = np.random.normal(100, 10, len(dates)).cumsum()
        df = pd.DataFrame({
            'open': prices,
            'high': prices + 2,
            'low': prices - 2,
            'close': prices,
            'volume': np.random.randint(1000, 10000, len(dates)),
        }, index=dates)
    
    # 确保有必要的列
    if 'close' not in df.columns:
        df['close'] = df['单位净值'] if '单位净值' in df.columns else df['close']
    
    # 创建OHLCV数据 - 基金通常只有净值，没有分时数据
    if 'open' not in df.columns:
        df['open'] = df['close']
    if 'high' not in df.columns:
        df['high'] = df['close']
    if 'low' not in df.columns:
        df['low'] = df['close']
    if 'volume' not in df.columns:
        df['volume'] = 0
    
    df['openinterest'] = 0
    
    # 筛选日期范围
    df = df.loc[start:end]
    
    # 确保数据按日期排序
    df.sort_index(inplace=True)
    
    print(f"数据长度: {len(df)}")
    print(f"开始日期: {df.index[0]}, 结束日期: {df.index[-1]}")
    
    return df[['open', 'high', 'low', 'close', 'volume', 'openinterest']]

def run_strategy():
    # 设置回测参数
    start = datetime(2022, 1, 1)
    end = datetime(2024, 9, 30)
    
    # 获取基金数据
    dataframe = get_data("002910", start=start.strftime('%Y-%m-%d'), end=极d.strftime('%Y-%m-%d'))
    
    # 检查数据是否为空
    if dataframe.empty:
        print("错误：无法获取数据，退出程序")
        return
    
    # 创建Data Feed
    data = bt.feeds.PandasData(
        dataname=dataframe,
        datetime=None,  # 使用索引作为日期
        open=0, high=1, low=2, close=3, volume=4, openinterest=5
    )
    
    # 创建Cerebro引擎
    cerebro = bt.Cerebro()
    
    # 添加策略
    cerebro.addstrategy(MACDStrategy)
    
    # 添加数据
    cerebro.adddata(data)
    
    # 设置初始资金
    cerebro.broker.setcash(10000)
    
    # 设置佣金
    cerebro.broker.setcommission(commission=0.0005)
    
    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    
    print("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())
    
    # 运行策略
    results = cerebro.run()
    
    # 打印最终结果
    print("Final Portfolio Value: %.2f" % cerebro.broker.getvalue())
    
    # 打印分析结果
    strat = results[0]
    print('年化回报: %.2f%%' % (strat.analyzers.returns.get_analysis()['rnorm100']))
    print('夏普比率: %.2f' % strat.analyzers.sharpe.get_analysis()['sharperatio'])
    print('最大回撤: %.2f%%' % strat.analyzers.drawdown.get_analysis()['极']['drawdown'])
    
    # 绘制图表
    cerebro.plot(style='candlestick', volume=False)

if __name__ == "__main__":
    run_strategy()
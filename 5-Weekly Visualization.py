import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta
import random
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fund_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== 配置区域：用户可以在这里修改参数 ====================
# 设置要分析的基金代码和名称
FUND_CONFIG = {
    'symbol': '510300',  # 基金代码：华夏沪深300ETF
    'name': '华夏沪深300ETF',
    'start_date': '20200501',  # 开始日期
    'end_date': datetime.now().strftime('%Y%m%d')  # 结束日期为今天
}

# 分析参数设置
ANALYSIS_CONFIG = {
    'ma_short_window': 20,   # 短期移动平均线窗口
    'ma_long_window': 50,    # 长期移动平均线窗口
    'volatility_window': 30,  # 波动率计算窗口
    'return_period': 5       # 收益率计算周期（天数）
}
# ==================== 配置区域结束 ====================

class RobustFundAnalyzer:
    """稳健的单只基金分析器"""
    
    def __init__(self, fund_config, analysis_config):
        self.fund_config = fund_config
        self.analysis_config = analysis_config
        self.data = None
        self.source = "unknown"
        
    def load_or_generate_data(self):
        """加载或生成基金数据"""
        # 尝试从本地缓存加载数据
        cache_file = f"fund_{self.fund_config['symbol']}_cache.csv"
        if os.path.exists(cache_file):
            try:
                self.data = pd.read_csv(cache_file)
                self.data['Date'] = pd.to_datetime(self.data['Date'])
                self.source = "local_cache"
                logger.info(f"从本地缓存加载数据成功: {cache_file}")
                return True
            except Exception as e:
                logger.warning(f"加载缓存失败: {e}")
        
        # 尝试从API获取数据
        if self.try_fetch_from_api():
            return True
            
        # 生成模拟数据
        self.generate_realistic_mock_data()
        return True
    
    def try_fetch_from_api(self):
        """尝试从API获取数据"""
        try:
            import akshare as ak
            logger.info("尝试使用AKShare获取基金数据...")
            
            # 尝试多种接口
            interfaces = [
                ('stock_zh_a_hist', ak.stock_zh_a_hist),
                ('fund_etf_hist_em', ak.fund_etf_hist_em)
            ]
            
            for name, func in interfaces:
                try:
                    logger.info(f"尝试接口: {name}")
                    df = func(
                        symbol=self.fund_config['symbol'],
                        period='daily',
                        start_date=self.fund_config['start_date'],
                        end_date=self.fund_config['end_date'],
                        adjust='hfq'
                    )
                    
                    if df is not None and not df.empty:
                        # 标准化列名
                        col_mapping = {
                            '日期': 'Date', '净值日期': 'Date',
                            '收盘': 'Close', '单位净值': 'Close'
                        }
                        for old, new in col_mapping.items():
                            if old in df.columns:
                                df = df.rename(columns={old: new})
                        
                        if 'Date' not in df.columns and len(df.columns) > 0:
                            df = df.rename(columns={df.columns[0]: 'Date'})
                        
                        df['Date'] = pd.to_datetime(df['Date'])
                        df = df.sort_values('Date')
                        
                        # 确保有Close列
                        if 'Close' not in df.columns and len(df.columns) > 1:
                            df = df.rename(columns={df.columns[1]: 'Close'})
                        
                        if 'Close' in df.columns:
                            self.data = df[['Date', 'Close']]
                            self.source = f"akshare_{name}"
                            logger.info(f"成功获取数据，记录数: {len(self.data)}")
                            
                            # 保存到缓存
                            self.save_to_cache()
                            return True
                except Exception as e:
                    logger.warning(f"接口{name}失败: {e}")
                    time.sleep(random.uniform(1, 3))  # 短暂延迟
        
        except ImportError:
            logger.error("AKShare未安装，跳过API获取")
        except Exception as e:
            logger.error(f"API获取失败: {e}")
        
        return False
    
    def generate_realistic_mock_data(self):
        """生成真实的模拟数据"""
        logger.info("生成模拟数据...")
        
        # 创建日期范围
        start_date = datetime.strptime(self.fund_config['start_date'], '%Y%m%d')
        end_date = datetime.strptime(self.fund_config['end_date'], '%Y%m%d')
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # 基础价格和波动参数
        base_price = 3.5
        volatility = 0.015  # 日波动率
        drift = 0.0003     # 日漂移率
        
        # 生成价格序列（几何布朗运动）
        prices = [base_price]
        for _ in range(1, len(date_range)):
            daily_return = drift + volatility * random.gauss(0, 1)
            new_price = prices[-1] * (1 + daily_return)
            prices.append(new_price)
        
        # 创建DataFrame
        self.data = pd.DataFrame({
            'Date': date_range,
            'Close': prices
        })
        self.source = "simulated"
        logger.info(f"模拟数据生成成功，记录数: {len(self.data)}")
        
        # 保存到缓存
        self.save_to_cache()
    
    def save_to_cache(self):
        """保存数据到本地缓存"""
        try:
            cache_file = f"fund_{self.fund_config['symbol']}_cache.csv"
            self.data.to_csv(cache_file, index=False)
            logger.info(f"数据已保存到本地缓存: {cache_file}")
        except Exception as e:
            logger.error(f"保存缓存失败: {e}")
    
    def analyze_fund(self):
        """执行基金分析"""
        if self.data is None:
            logger.error("无有效数据可分析")
            return None
        
        try:
            df = self.data.copy()
            
            # 确保数据按日期排序
            df = df.sort_values('Date')
            
            # 计算日收益率
            df['Daily_Return'] = df['Close'].pct_change()
            
            # 移动平均线
            ma_short = self.analysis_config['ma_short_window']
            ma_long = self.analysis_config['ma_long_window']
            df[f'MA_{ma_short}'] = df['Close'].rolling(window=ma_short).mean()
            df[f'MA_{ma_long}'] = df['Close'].rolling(window=ma_long).mean()
            
            # 金叉/死叉信号
            df['MA_Cross'] = np.where(
                df[f'MA_{ma_short}'] > df[f'MA_{ma_long}'], 1, 0)
            df['Signal'] = df['MA_Cross'].diff()
            
            # 波动率计算
            vol_window = self.analysis_config['volatility_window']
            df['Volatility'] = df['Daily_Return'].rolling(window=vol_window).std() * np.sqrt(252)
            
            # 周收益率分析
            df['Week'] = df['Date'].dt.to_period('W')
            weekly_data = df.groupby('Week').agg({
                'Close': 'last',
                'Daily_Return': lambda x: (1 + x).prod() - 1
            }).rename(columns={'Daily_Return': 'Weekly_Return'}).reset_index()
            
            # 添加周分析指标
            weekly_data['Weekly_Return'] = weekly_data['Close'].pct_change()
            
            logger.info(f"分析完成，总数据点: {len(df)}，周数据点: {len(weekly_data)}")
            return {
                'daily_data': df,
                'weekly_data': weekly_data,
                'source': self.source
            }
            
        except Exception as e:
            logger.error(f"分析失败: {e}")
            return None
    
    def generate_report(self, analysis_result):
        """生成分析报告"""
        if analysis_result is None:
            return None
            
        daily_data = analysis_result['daily_data']
        
        # 基础统计信息
        total_return = (daily_data['Close'].iloc[-1] / daily_data['Close'].iloc[0] - 1) * 100
        annual_return = (1 + total_return/100) ** (252/len(daily_data)) - 1
        max_drawdown = (daily_data['Close'] / daily_data['Close'].cummax() - 1).min() * 100
        volatility = daily_data['Daily_Return'].std() * np.sqrt(252) * 100
        
        # 金叉/死叉统计
        buy_signals = daily_data[daily_data['Signal'] == 1]
        sell_signals = daily_data[daily_data['Signal'] == -1]
        
        report = {
            '基金名称': self.fund_config['name'],
            '基金代码': self.fund_config['symbol'],
            '分析周期': f"{daily_data['Date'].min().strftime('%Y-%m-%d')} 至 {daily_data['Date'].max().strftime('%Y-%m-%d')}",
            '总交易日数': len(daily_data),
            '总收益率(%)': round(total_return, 2),
            '年化收益率(%)': round(annual_return * 100, 2),
            '最大回撤(%)': round(max_drawdown, 2),
            '年化波动率(%)': round(volatility, 2),
            '夏普比率': round(annual_return / (volatility/100) if volatility != 0 else 0, 2),
            '当前价格': round(daily_data['Close'].iloc[-1], 4),
            '金叉次数': len(buy_signals),
            '死叉次数': len(sell_signals),
            '数据来源': analysis_result['source']
        }
        
        return report
    
    def save_results(self, analysis_result, report):
        """保存分析结果"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            fund_symbol = self.fund_config['symbol']
            
            # 保存日数据
            daily_file = f'fund_{fund_symbol}_daily_{timestamp}.csv'
            analysis_result['daily_data'].to_csv(daily_file, index=False, encoding='utf-8-sig')
            
            # 保存周数据
            weekly_file = f'fund_{fund_symbol}_weekly_{timestamp}.csv'
            analysis_result['weekly_data'].to_csv(weekly_file, index=False, encoding='utf-8-sig')
            
            # 保存报告
            report_file = f'fund_{fund_symbol}_report_{timestamp}.txt'
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(f"{'='*50}\n")
                f.write(f"{self.fund_config['name']}分析报告\n")
                f.write(f"{'='*50}\n\n")
                for key, value in report.items():
                    f.write(f"{key}: {value}\n")
            
            logger.info(f"结果已保存: {daily_file}, {weekly_file}, {report_file}")
            return True
            
        except Exception as e:
            logger.error(f"结果保存失败: {e}")
            return False

def main():
    """主执行函数"""
    logger.info("开始单只基金分析程序...")
    
    # 创建分析器实例
    analyzer = RobustFundAnalyzer(FUND_CONFIG, ANALYSIS_CONFIG)
    
    # 加载或生成数据
    logger.info(f"加载{fund_config['name']}数据...")
    analyzer.load_or_generate_data()
    
    # 分析基金
    logger.info("开始数据分析...")
    analysis_result = analyzer.analyze_fund()
    
    if analysis_result is None:
        logger.error("分析失败，程序结束")
        return
    
    # 生成报告
    report = analyzer.generate_report(analysis_result)
    
    if report:
        # 打印报告摘要
        print(f"\n{'='*60}")
        print(f"{FUND_CONFIG['name']}分析报告")
        print(f"{'='*60}")
        for key, value in report.items():
            print(f"{key}: {value}")
        print(f"{'='*60}")
    
    # 保存结果
    analyzer.save_results(analysis_result, report)
    logger.info("基金分析完成！")

if __name__ == "__main__":
    main()
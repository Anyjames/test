import akshare as ak
import pandas as pd
import time
import random
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FundDataFetcher:
    def __init__(self):
        self.request_count = 0
        self.last_request_time = 0
        
    def _safety_delay(self):
        """智能延迟控制，避免触发反爬机制"""
        current_time = time.time()
        if current_time - self.last_request_time < 3:  # 至少3秒间隔
            sleep_time = random.uniform(3, 8)
            logger.info(f"等待{sleep_time:.2f}秒以避免频率限制")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        # 每10次请求后长休息
        if self.request_count % 10 == 0:
            long_break = random.uniform(10, 15)
            logger.info(f"已完成{self.request_count}次请求，休息{long_break:.2f}秒")
            time.sleep(long_break)
    
    def fetch_with_retry(self, fetch_func, max_retries=3, *args, **kwargs):
        """带重试机制的数据获取"""
        for attempt in range(max_retries):
            try:
                self._safety_delay()
                logger.info(f"尝试 {attempt+1}/{max_retries}: {fetch_func.__name__}")
                
                result = fetch_func(*args, **kwargs)
                
                if result is None or result.empty:
                    logger.warning(f"第{attempt+1}次尝试返回空数据")
                    if attempt == max_retries - 1:
                        return None
                    time.sleep(random.uniform(5, 10))
                    continue
                    
                logger.info(f"数据获取成功，形状: {result.shape}")
                return result
                
            except Exception as e:
                logger.error(f"第{attempt+1}次尝试失败: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(random.uniform(5, 10))
        return None

def get_fund_data_optimized():
    """优化的基金数据获取函数"""
    fetcher = FundDataFetcher()
    
    try:
        # 1. 获取华夏沪深300ETF数据
        logger.info("开始获取华夏沪深300ETF数据...")
        fund1 = fetcher.fetch_with_retry(
            ak.fund_etf_hist_em,
            symbol="510300", 
            period="daily",
            start_date="20200501", 
            end_date="20251123",  # 调整为实际日期
            adjust="hfq"
        )
        
        # 2. 获取易方达消费行业股票型基金数据（使用开放式基金接口）
        logger.info("开始获取易方达消费行业股票型基金数据...")
        fund2 = fetcher.fetch_with_retry(
            ak.fund_em_open_fund_info,
            fund="110022", 
            indicator="单位净值走势"
        )
        
        # 3. 获取沪深300指数数据作为基准
        logger.info("开始获取沪深300指数数据...")
        index = fetcher.fetch_with_retry(
            ak.index_zh_a_hist,
            symbol="000300", 
            period="daily",
            start_date="20200501", 
            end_date="20251123"
        )
        
        # 数据预处理和重命名
        data_frames = []
        
        if fund1 is not None:
            # 检查并适配ETF数据的列名
            print("华夏沪深300ETF数据列名:", fund1.columns.tolist())
            if '日期' in fund1.columns and '收盘' in fund1.columns:
                fund1 = fund1.rename(columns={'日期': 'Date', '收盘': 'Fund1_Close'})
            elif '净值日期' in fund1.columns and '单位净值' in fund1.columns:
                fund1 = fund1.rename(columns={'净值日期': 'Date', '单位净值': 'Fund1_Close'})
            data_frames.append(fund1[['Date', 'Fund1_Close']])
        
        if fund2 is not None:
            # 处理开放式基金数据
            print("易方达消费行业基金数据列名:", fund2.columns.tolist())
            if '净值日期' in fund2.columns and '单位净值' in fund2.columns:
                fund2 = fund2.rename(columns={'净值日期': 'Date', '单位净值': 'Fund2_Close'})
            data_frames.append(fund2[['Date', 'Fund2_Close']])
        
        if index is not None:
            # 处理指数数据
            index = index.rename(columns={'日期': 'Date', '收盘': 'Index_Close'})
            data_frames.append(index[['Date', 'Index_Close']])
        
        # 合并所有可用的数据
        if len(data_frames) == 0:
            logger.error("未能获取到任何数据")
            return None
        
        # 逐步合并数据
        merged_data = data_frames[0]
        for i in range(1, len(data_frames)):
            merged_data = pd.merge(merged_data, data_frames[i], on='Date', how='outer')
        
        # 数据处理和计算周收益率
        result = process_weekly_data(merged_data)
        
        if result is not None:
            # 保存结果
            result.to_csv('funds_weekly_data.csv', index=False)
            logger.info("基金数据获取和保存成功完成！")
            return result
        else:
            logger.error("数据处理失败")
            return None
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return None

def process_weekly_data(data):
    """处理数据并计算周收益率"""
    try:
        # 确保日期格式正确
        data['Date'] = pd.to_datetime(data['Date'])
        data = data.sort_values('Date').dropna(subset=['Date'])
        
        # 计算周收益率
        data['Week'] = data['Date'].dt.to_period('W')
        weekly_data = data.groupby('Week').last().reset_index()
        
        # 计算各基金的周收益率
        for col in ['Fund1_Close', 'Fund2_Close', 'Index_Close']:
            if col in weekly_data.columns:
                return_col = col.replace('_Close', '_Return')
                weekly_data[return_col] = weekly_data[col].pct_change()
        
        # 删除包含NaN的行
        weekly_data = weekly_data.dropna()
        
        logger.info(f"最终数据形状: {weekly_data.shape}")
        return weekly_data
        
    except Exception as e:
        logger.error(f"数据处理失败: {e}")
        return None

def main():
    """主函数"""
    logger.info("开始执行基金数据获取程序...")
    
    result = get_fund_data_optimized()
    
    if result is not None:
        print("\n数据获取成功！")
        print(f"数据时间范围: {result['Date'].min()} 至 {result['Date'].max()}")
        print(f"总周数: {len(result)}")
        
        # 显示各列的统计信息
        for col in ['Fund1_Return', 'Fund2_Return', 'Index_Return']:
            if col in result.columns:
                print(f"{col} 统计:")
                print(f"  均值: {result[col].mean():.4f}")
                print(f"  标准差: {result[col].std():.4f}")
                print(f"  最大周收益: {result[col].max():.4f}")
                print(f"  最小周收益: {result[col].min():.4f}")
                print()
                
    else:
        print("数据获取失败，请检查网络连接或稍后重试")

if __name__ == "__main__":
    main()
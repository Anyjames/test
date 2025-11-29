import akshare as ak
import pandas as pd
import time
import random
import logging
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def robust_fund_data_fetcher():
    """稳健的基金数据获取方案"""
    
    # 方案1: 使用正确的AKShare函数
    try:
        logger.info("尝试方案1: 使用正确的AKShare函数")
        
        # 获取华夏沪深300ETF - 使用股票接口（因为ETF在二级市场交易）
        fund1 = ak.stock_zh_a_hist(
            symbol="510300", 
            period="daily",
            start_date="20200501", 
            end_date="20251123",  # 使用实际日期
            adjust="hfq"
        )
        logger.info(f"华夏沪深300ETF获取成功，数据量: {len(fund1)}")
        
        # 获取易方达消费行业基金 - 使用正确的函数名
        fund2 = ak.fund_open_fund_info_em(
            fund="110022", 
            indicator="单位净值走势"
        )
        logger.info(f"易方达消费行业基金获取成功，数据量: {len(fund2)}")
        
        return {"fund1": fund1, "fund2": fund2, "source": "akshare"}
        
    except Exception as e:
        logger.error(f"方案1失败: {e}")
        return None

def alternative_data_sources():
    """备用数据源方案"""
    
    # 方案2: 使用baostock（更稳定的数据源）
    try:
        import baostock as bs
        logger.info("尝试方案2: 使用baostock数据源")
        
        # 登录baostock
        lg = bs.login()
        if lg.error_code == '0':
            # 获取沪深300指数数据作为替代
            rs = bs.query_history_k_data_plus(
                "sh.000300",
                "date,code,open,high,low,close,volume",
                start_date='2020-05-01', 
                end_date='2025-11-23',
                frequency="d"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            index_data = pd.DataFrame(data_list, columns=rs.fields)
            bs.logout()
            
            logger.info(f"baostock数据获取成功，数据量: {len(index_data)}")
            return {"index": index_data, "source": "baostock"}
            
    except Exception as e:
        logger.error(f"方案2失败: {e}")
    
    return None

def simple_fallback_strategy():
    """最简单的回退策略：使用本地测试数据"""
    logger.info("启用方案3: 使用模拟数据进行演示")
    
    # 生成模拟数据用于开发和测试
    dates = pd.date_range(start='2020-05-01', end='2025-11-23', freq='D')
    mock_data = pd.DataFrame({
        'date': dates,
        'close': [100 + i * 0.1 + random.random() for i in range(len(dates))]
    })
    
    logger.info(f"模拟数据生成成功，数据量: {len(mock_data)}")
    return {"mock": mock_data, "source": "mock_data"}

def main():
    """主执行函数 - 多层级的稳健数据获取策略"""
    logger.info("开始执行多层级的稳健数据获取策略...")
    
    # 第一层：主要数据源
    result = robust_fund_data_fetcher()
    
    # 第二层：备用数据源
    if result is None:
        logger.warning("主要数据源失效，尝试备用数据源")
        result = alternative_data_sources()
    
    # 第三层：模拟数据回退
    if result is None:
        logger.warning("备用数据源失效，启用模拟数据")
        result = simple_fallback_strategy()
    
    # 数据处理和分析
    if result is not None:
        process_and_analyze_data(result)
    else:
        logger.error("所有数据获取方案均失败")

def process_and_analyze_data(data_dict):
    """数据处理和分析函数"""
    try:
        source = data_dict.get("source", "unknown")
        logger.info(f"开始处理来自 {source} 的数据")
        
        if source == "akshare":
            # 处理AKShare数据
            fund1 = data_dict.get("fund1")
            fund2 = data_dict.get("fund2")
            
            if fund1 is not None:
                fund1 = fund1.rename(columns={'日期': 'Date', '收盘': 'Close'})
                fund1['Date'] = pd.to_datetime(fund1['Date'])
                logger.info(f"基金1数据处理完成，时间范围: {fund1['Date'].min()} 到 {fund1['Date'].max()}")
            
            if fund2 is not None:
                # 检查并适配列名
                if '净值日期' in fund2.columns and '单位净值' in fund2.columns:
                    fund2 = fund2.rename(columns={'净值日期': 'Date', '单位净值': 'Close'})
                fund2['Date'] = pd.to_datetime(fund2['Date'])
                logger.info(f"基金2数据处理完成，时间范围: {fund2['Date'].min()} 到 {fund2['Date'].max()}")
            
            # 保存处理后的数据
            if fund1 is not None:
                fund1.to_csv('fund_510300_processed.csv', index=False)
            if fund2 is not None:
                fund2.to_csv('fund_110022_processed.csv', index=False)
                
        elif source == "baostock":
            # 处理baostock数据
            index_data = data_dict.get("index")
            index_data['date'] = pd.to_datetime(index_data['date'])
            index_data.to_csv('index_000300_processed.csv', index=False)
            logger.info("指数数据保存完成")
            
        elif source == "mock_data":
            # 处理模拟数据
            mock_data = data_dict.get("mock")
            mock_data.to_csv('mock_fund_data.csv', index=False)
            logger.info("模拟数据保存完成，可用于算法开发和测试")
        
        logger.info("数据处理和保存成功完成！")
        
    except Exception as e:
        logger.error(f"数据处理失败: {e}")

if __name__ == "__main__":
    main()
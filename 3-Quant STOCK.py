import requests
from bs4 import BeautifulSoup
import random
import time
import csv
from datetime import datetime
import re
import json
import os
import sys

class EastMoneyGubaCrawler:
    def __init__(self, stock_code="002594"):
        self.stock_code = stock_code
        self.base_url = "https://guba.eastmoney.com"
        
        # 增强的浏览器指纹库
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
        ]
        
        # 代理IP池（需要用户自行配置）
        self.proxies = [
            # 示例格式，实际使用时需要替换为有效代理
            # 'http://user:pass@ip:port',
            # 'http://ip:port'
        ]
        
        self.session = requests.Session()
        self.update_headers()
        
        # 访问统计
        self.request_count = 0
        self.last_request_time = 0

    def update_headers(self):
        """更新请求头部模拟真实浏览器"""
        self.headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://www.eastmoney.com/',
            'X-Forwarded-For': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
        }
        self.session.headers.update(self.headers)

    def smart_delay(self):
        """智能延时控制请求频率"""
        current_time = time.time()
        if self.request_count > 0:
            time_since_last = current_time - self.last_request_time
            if time_since_last < 5:  # 至少5秒间隔
                sleep_time = max(5 - time_since_last, 0) + random.uniform(2, 6)
                time.sleep(sleep_time)
        
        self.request_count += 1
        self.last_request_time = time.time()

    def get_page_with_advanced_retry(self, url, max_retries=5):
        """高级重试机制应对反爬虫"""
        for attempt in range(max_retries):
            try:
                self.smart_delay()
                
                # 动态更新头部
                self.update_headers()
                
                # 为重试请求添加不同的Referer
                if attempt > 0:
                    page_num = re.search(r'_(\d+)\.html', url)
                    if page_num:
                        ref_page = int(page_num.group(1)) - 1 if int(page_num.group(1)) > 1 else 1
                        self.headers['Referer'] = f"{self.base_url}/list,{self.stock_code}_{ref_page}.html"
                
                # 随机选择代理
                proxy = random.choice(self.proxies) if self.proxies else None
                
                response = self.session.get(url, timeout=15, proxies={'http': proxy, 'https': proxy} if proxy else None)
                response.raise_for_status()
                
                # 反爬虫检测
                if self.is_blocked(response):
                    print(f"第{attempt+1}次尝试被反爬虫拦截")
                    continue
                
                if self.is_valid_content(response):
                    return response
                else:
                    print(f"第{attempt+1}次返回内容无效")
                    continue
                    
            except requests.RequestException as e:
                print(f"第{attempt+1}次请求失败: {e}")
        
        return None

    def is_blocked(self, response):
        """检测是否被反爬虫拦截"""
        blocked_indicators = [
            response.status_code == 403,
            response.status_code == 429,
            'access denied' in response.text.lower(),
            'forbidden' in response.text.lower(),
            '验证' in response.text,
            '反爬虫' in response.text,
            'challenge' in response.text.lower(),
            len(response.text) < 3000  # 内容过短
        ]
        return any(blocked_indicators)

    def is_valid_content(self, response):
        """验证返回内容有效性"""
        content_indicators = [
            'articlelistnew' in response.text,
            'articleh' in response.text,
            self.stock_code in response.text,
            len(response.text) > 5000  # 合理的内容长度
        ]
        return any(content_indicators)  # 降低验证阈值

    def parse_guba_page_advanced(self, html, page):
        """高级页面解析策略"""
        soup = BeautifulSoup(html, 'html.parser')
        posts = []
        
        # 策略1: 标准结构解析
        article_list = soup.find('div', id='articlelistnew')
        if article_list:
            standard_posts = self.parse_standard_structure(article_list, page)
            posts.extend(standard_posts)
            print(f"策略1解析到 {len(standard_posts)} 条帖子")
        
        # 策略2: 全面搜索articleh元素
        all_articleh = soup.find_all('div', class_='articleh')
        additional_posts = []
        for item in all_articleh:
            post_data = self.parse_article_item(item, page)
            if post_data and not any(p['title'] == post_data['title'] for p in posts):
                additional_posts.append(post_data)
        
        if additional_posts:
            posts.extend(additional_posts)
            print(f"策略2新增 {len(additional_posts)} 条帖子")
        
        # 策略3: 链接模式匹配
        pattern_links = soup.find_all('a', href=re.compile(r'news,' + self.stock_code))
        link_posts = []
        for link in pattern_links:
            post_data = self.parse_from_link(link, page)
            if post_data and not any(p['title'] == post_data['title'] for p in posts):
                link_posts.append(post_data)
        
        if link_posts:
            posts.extend(link_posts)
            print(f"策略3新增 {len(link_posts)} 条帖子")
        
        # 策略4: 备用解析方法
        backup_posts = self.parse_backup_structure(soup, page)
        if backup_posts:
            posts.extend(backup_posts)
            print(f"策略4新增 {len(backup_posts)} 条帖子")
        
        # 策略5: 标题文本解析
        title_divs = soup.find_all(['div', 'span'], class_=re.compile(r'title|l3'))
        title_posts = []
        for div in title_divs:
            post_data = self.parse_from_title_div(div, page)
            if post_data and not any(p['title'] == post_data['title'] for p in posts):
                title_posts.append(post_data)
        
        if title_posts:
            posts.extend(title_posts)
            print(f"策略5新增 {len(title_posts)} 条帖子")
        
        # 策略6: 列表项解析
        list_items = soup.find_all('li', class_=re.compile(r'list_item|post_item'))
        list_posts = []
        for item in list_items:
            post_data = self.parse_from_list_item(item, page)
            if post_data and not any(p['title'] == post_data['title'] for p in posts):
                list_posts.append(post_data)
        
        if list_posts:
            posts.extend(list_posts)
            print(f"策略6新增 {len(list_posts)} 条帖子")
        
        # 去重处理
        unique_posts = []
        seen_titles = set()
        for post in posts:
            if post['title'] and len(post['title']) > 5 and post['title'] not in seen_titles:
                unique_posts.append(post)
                seen_titles.add(post['title'])
        
        print(f"第{page}页去重后获取 {len(unique_posts)} 条有效帖子")
        return unique_posts

    def parse_standard_structure(self, article_list, page):
        """解析标准页面结构"""
        posts = []
        article_items = article_list.find_all('div', class_='articleh')
        
        for item in article_items:
            post_data = self.parse_article_item(item, page)
            if post_data:
                posts.append(post_data)
        
        return posts

    def parse_backup_structure(self, soup, page):
        """备用解析方法"""
        posts = []
        # 尝试解析div.title元素
        title_divs = soup.find_all('div', class_='title')
        for div in title_divs:
            link = div.find('a')
            if link:
                title = link.get_text(strip=True)
                if title and len(title) > 5:
                    posts.append({
                        'title': title,
                        'link': self.base_url + link['href'] if not link['href'].startswith('http') else link['href'],
                        'read_count': 0,
                        'comment_count': 0,
                        'author': "未知",
                        'time': "",
                        'page': page,
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
        return posts

    def parse_from_title_div(self, div, page):
        """从标题div解析"""
        try:
            link = div.find('a')
            if link:
                title = link.get_text(strip=True)
                if title and len(title) > 5:
                    return {
                        'title': title,
                        'link': self.base_url + link['href'] if not link['href'].startswith('http') else link['href'],
                        'read_count': 0,
                        'comment_count': 0,
                        'author': "未知",
                        'time': "",
                        'page': page,
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
        except:
            return None

    def parse_from_list_item(self, item, page):
        """从列表项解析"""
        try:
            # 查找标题链接
            link = item.find('a', href=re.compile(r'news,' + self.stock_code))
            if not link:
                return None
                
            title = link.get_text(strip=True)
            if not title or len(title) < 5:
                return None
            
            # 提取阅读数和评论数
            read_count = 0
            comment_count = 0
            read_elem = item.find('span', class_=re.compile(r'read|click'))
            if read_elem:
                read_text = read_elem.get_text(strip=True)
                read_count = self.parse_number(read_text)
            
            comment_elem = item.find('span', class_=re.compile(r'comment|reply'))
            if comment_elem:
                comment_text = comment_elem.get_text(strip=True)
                comment_count = self.parse_number(comment_text)
            
            # 提取作者和时间
            author = "未知"
            author_elem = item.find('span', class_=re.compile(r'author|user'))
            if author_elem:
                author = author_elem.get_text(strip=True)
            
            post_time = ""
            time_elem = item.find('span', class_=re.compile(r'time|date'))
            if time_elem:
                post_time = time_elem.get_text(strip=True)
            
            return {
                'title': title,
                'link': self.base_url + link['href'] if not link['href'].startswith('http') else link['href'],
                'read_count': read_count,
                'comment_count': comment_count,
                'author': author,
                'time': post_time,
                'page': page,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except:
            return None

    def parse_article_item(self, item, page):
        """解析单个文章项"""
        try:
            # 标题提取
            title_span = item.find('span', class_='l3')
            if not title_span:
                return None
                
            link_elem = title_span.find('a')
            if not link_elem:
                return None
                
            title = link_elem.get_text(strip=True)
            if not title or len(title) < 5:
                return None
            
            # 链接处理
            link = link_elem.get('href', '')
            if link and not link.startswith('http'):
                link = self.base_url + link if not link.startswith('//') else 'https:' + link
            
            # 数字信息提取
            read_span = item.find('span', class_='l1')
            read_text = read_span.get_text(strip=True) if read_span else "0"
            read_count = self.parse_number(read_text)
            
            comment_span = item.find('span', class_='l2')
            comment_text = comment_span.get_text(strip=True) if comment_span else "0"
            comment_count = self.parse_number(comment_text)
            
            # 作者和时间
            author_span = item.find('span', class_='l4')
            author = author_span.get_text(strip=True) if author_span else "未知"
            
            time_span = item.find('span', class_='l5')
            post_time = time_span.get_text(strip=True) if time_span else ""
            
            return {
                'title': title,
                'link': link,
                'read_count': read_count,
                'comment_count': comment_count,
                'author': author,
                'time': post_time,
                'page': page,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            return None

    def parse_from_link(self, link_elem, page):
        """从链接元素解析"""
        try:
            title = link_elem.get_text(strip=True)
            if not title or len(title) < 5:
                return None
                
            link = link_elem.get('href', '')
            if link and not link.startswith('http'):
                link = self.base_url + link if not link.startswith('//') else 'https:' + link
            
            return {
                'title': title,
                'link': link,
                'read_count': 0,
                'comment_count': 0,
                'author': "未知",
                'time': "",
                'page': page,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except:
            return None

    def parse_number(self, text):
        """解析数字文本"""
        if not text:
            return 0
        
        text = text.strip()
        if '万' in text:
            numbers = re.findall(r'\d+\.?\d*', text)
            return int(float(numbers[0]) * 10000) if numbers else 0
        elif '亿' in text:
            numbers = re.findall(r'\d+\.?\d*', text)
            return int(float(numbers[0]) * 100000000) if numbers else 0
        else:
            numbers = re.findall(r'\d+', text)
            return int(numbers[0]) if numbers else 0

    def crawl_multiple_pages(self, start_page=1, end_page=3):
        """爬取多页数据"""
        all_posts = []
        
        print(f"开始爬取第{start_page}到{end_page}页数据...")
        
        for page in range(start_page, end_page + 1):
            print(f"正在处理第 {page} 页...")
            
            if page == 1:
                url = f"{self.base_url}/list,{self.stock_code}.html"
            else:
                url = f"{self.base_url}/list,{self.stock_code}_{page}.html"
            
            response = self.get_page_with_advanced_retry(url)
            
            if response:
                posts = self.parse_guba_page_advanced(response.text, page)
                if posts:
                    all_posts.extend(posts)
                    print(f"第{page}页成功获取 {len(posts)} 条帖子")
                else:
                    print(f"第{page}页解析到0条帖子")
            else:
                print(f"第{page}页获取失败")
            
            # 页面间延时
            if page < end_page:
                time.sleep(random.uniform(5, 10))  # 增加延时
            
        print(f"总计获取 {len(all_posts)} 条帖子")
        return all_posts

    def save_to_csv(self, posts, filename=None):
        """保存数据到CSV"""
        if not posts:
            print("没有数据可保存")
            return None
            
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"guba_data_{self.stock_code}_{timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                fieldnames = ['title', 'link', 'read_count', 'comment_count', 'author', 'time', 'page', 'crawl_time']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(posts)
            
            print(f"数据已保存到: {filename}")
            return filename
        except Exception as e:
            print(f"保存失败: {e}")
            return None

    def analyze_sentiment(self, posts):
        """简单情感分析"""
        if not posts:
            return {"sentiment": "neutral", "confidence": 0.5}
        
        titles = [post['title'] for post in posts[:100]]
        
        positive_words = ["看好", "推荐", "买入", "增长", "利好", "突破", "大涨", "持有", "加仓", "创新高", "超预期"]
        negative_words = ["卖出", "下跌", "利空", "谨慎", "观望", "调整", "风险", "亏损", "减持", "破位", "回调"]
        
        positive_count = 0
        negative_count = 0
        
        for title in titles:
            title_lower = title.lower()
            for word in positive_words:
                if word in title_lower:
                    positive_count += 1
            for word in negative_words:
                if word in title_lower:
                    negative_count += 1
        
        total = positive_count + negative_count
        if total == 0:
            return {"sentiment": "neutral", "confidence": 0.5}
        elif positive_count > negative_count:
            confidence = min(positive_count / total, 0.95)
            return {"sentiment": "positive", "confidence": round(confidence, 2)}
        else:
            confidence = min(negative_count / total, 0.95)
            return {"sentiment": "negative", "confidence": round(confidence, 2)}

def test_crawler():
    """测试爬虫功能"""
    print("=== 东方财富股吧爬虫测试 ===")
    
    crawler = EastMoneyGubaCrawler("002594")
    
    # 测试单页爬取
    print("测试单页爬取...")
    test_posts = crawler.crawl_multiple_pages(1, 1)
    
    if test_posts:
        print(f"测试成功，获取 {len(test_posts)} 条帖子")
        print("前5条帖子示例:")
        for i, post in enumerate(test_posts[:5], 1):
            print(f"{i}. {post['title']}")
        
        # 保存测试数据
        filename = crawler.save_to_csv(test_posts)
        
        # 情感分析测试
        sentiment = crawler.analyze_sentiment(test_posts)
        print(f"情感分析结果: {sentiment}")
    else:
        print("测试失败，使用模拟数据演示")
        # 模拟数据演示
        mock_posts = [
            {
                'title': '比亚迪销量创新高，股价有望突破',
                'link': '',
                'read_count': 10000,
                'comment_count': 100,
                'author': '测试用户',
                'time': '今天 10:00',
                'page': 1,
                'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        ]
        crawler.save_to_csv(mock_posts, "mock_data.csv")
        print("模拟数据已保存")

if __name__ == "__main__":
    test_crawler()

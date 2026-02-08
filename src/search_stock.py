import time

from data_provider import DataFetcherManager

import akshare as ak
import pandas as pd
import asyncio
import logging
import random
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from src.stock_analyzer import StockTrendAnalyzer, TrendAnalysisResult, BuySignal
from src.storage import get_db

logger = logging.getLogger(__name__)


def get_all_stock_codes():
    """
    获取A股代码和名称
    
    逻辑：
    1. 尝试从数据库 stock_info 表加载（条件：list_status='L'）
    2. 如果数据库没有数据，则从缓存文件加载
    3. 如果缓存也没有，则从网络获取并保存到缓存
    """
    db = get_db()

    # 1. 尝试从数据库加载
    try:
        stock_infos = db.get_stock_info(list_status='L', market=['主板','创业板','科创板'])
        if stock_infos:
            logger.info(f"📋 从数据库加载了 {len(stock_infos)} 只在市股票")
            return [(info.symbol, info.name) for info in stock_infos]
    except Exception as e:
        logger.warning(f"从数据库加载股票列表失败: {e}")

    # 2. 尝试从缓存加载
    cached_tickers = load_cached_tickers()
    if cached_tickers:
        logger.info(f"📋 使用缓存的ticker列表，共 {len(cached_tickers)} 只股票")
        tickers = cached_tickers
    else:
        # 3. 从网络获取
        logger.warning("⚠️  未找到缓存的ticker列表，正在获取全部A股...")
        tickers = fetch_all_cn_stocks()
        save_tickers(tickers)

    # 过滤ETF/基金等非股票
    logger.info("正在过滤ETF和非股票类型...")
    original_count = len(tickers)
    filtered_tickers = [(symbol, name) for symbol, name in tickers if is_actual_cn_stock(symbol, name)]
    filtered_count = original_count - len(filtered_tickers)
    logger.info(f"📊 过滤掉 {filtered_count} 只ETF/基金/债券等，剩余 {len(filtered_tickers)} 只纯股票")

    return filtered_tickers


class StockAnalyzer:
    def __init__(self):
        self.LIMIT_UP_THRESHOLD = 9.5
        # 限制并发数为 5 (非常重要：过高会被封，建议 3-10 之间)
        self.semaphore = asyncio.Semaphore(4)
        # 线程池，用于执行 akshare 的同步代码，防止阻塞 FastAPI 主线程
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.fetcher_manager = DataFetcherManager()
        self.trend_analyzer = StockTrendAnalyzer()
        self.db = get_db()

    def scan_strong_stocks(self) -> list[TrendAnalysisResult]:
        """
        扫描全量股票数据得到强势股票
        
        逻辑：
        1. 获取所有股票代码
        2. 循环获取单股日线数据（使用 DataFetcherManager）
        3. 筛选符合条件的股票（市值、PE、换手率、涨停回调或温和放量）
        4. 保存结果
        """
        logger.info("开始扫描全市场强势股...")
        
        # 1. 获取所有股票代码
        all_stocks = get_all_stock_codes()
        stock_list = [code for code, _ in all_stocks]
        logger.info(f"获取到 {len(stock_list)} 只股票代码")
        
        # 2. 并发获取数据并分析
        strong_stocks = []
        
        # 使用 asyncio 运行并发任务
        try:
            # 尝试获取当前正在运行的事件循环
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 如果没有正在运行的事件循环，则创建一个新的
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        if loop.is_running():
            # 如果事件循环正在运行，则在一个新线程中运行新的事件循环
            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                return new_loop.run_until_complete(self._batch_analyze(stock_list))

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(run_in_new_loop)
                results = future.result()
        else:
            # 如果事件循环未运行，则正常使用 run_until_complete
            results = loop.run_until_complete(self._batch_analyze(stock_list))
        
        # 5. 筛选出强势股
        for res in results:
            if res:
                strong_stocks.append(res)
        
        logger.info(f"扫描完成，发现 {len(strong_stocks)} 只强势股")
        return strong_stocks

    async def _batch_analyze(self, stock_list):
        """批量并发分析"""
        tasks = []
        for code in stock_list:
            tasks.append(self._analyze_single_stock_async(code))
        return await asyncio.gather(*tasks)

    async def _analyze_single_stock_async(self, code):
        """异步分析单只股票"""
        async with self.semaphore:
            try:
                # 在线程池中执行同步的数据获取和分析
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    self.executor, 
                    self._analyze_single_stock_sync, 
                    code
                )
                return result
            except Exception as e:
                logger.error(f"分析股票 {code} 失败: {e}")
                return None

    def _analyze_single_stock_sync(self, code):
        """同步分析单只股票（在线程池中运行）"""
        try:
            # === 前提条件检查 ===
            
            # 获取实时行情以获取市值、PE、换手率
            quote = self.fetcher_manager.get_realtime_quote(code)
            if not quote:
                return None
                
            # 1. 市值筛选 (50亿 - 800亿)
            if quote.total_mv is None:
                return None
            market_value_yi = quote.total_mv / 100000000
            if not (50 <= market_value_yi <= 800):
                return None
            
            # 2. 动态市盈率筛选 (0 - 100)
            if quote.pe_ratio is None or not (0 < quote.pe_ratio <= 100):
                return None

            # 3. 日间换手率筛选 (> 5%)
            if quote.turnover_rate is None or quote.turnover_rate <= 5.0:
                return None

            # === 获取历史数据 ===
            # 需要足够的数据来判断120日高点，请求180天数据
            df, _ = self.fetcher_manager.get_daily_data(code, days=180)
            
            if df is None or df.empty or len(df) < 120:
                logger.warning(f"股票: {code} 历史数据不足")
                return None
            
            # 确保按日期升序
            df = df.sort_values('date', ascending=True)
            
            # === 策略条件检查 (满足其一即可) ===
            condition_met = False
            strategy_name = ""
            
            # --- 条件1: 涨停回调策略 ---
            # 1. 近15个交易日内有1-2次涨停
            recent_15 = df.iloc[-15:]
            limit_up_count = len(recent_15[recent_15['pct_chg'] >= 9.5])
            
            if 1 <= limit_up_count <= 2:
                # 2. 近15个交易日的收盘价不是近120个交易日的高点
                # (理解为近15日最高收盘价 < 近120日最高收盘价，留一点buffer)
                recent_15_high = recent_15['close'].max()
                recent_120_high = df.iloc[-120:]['close'].max()
                
                if recent_15_high < recent_120_high * 0.99: # 稍微严格一点，不能等于
                    # 3. 当日收盘价距近15个交易日的回调幅度小于10%
                    # 回调幅度 = (近15日最高 - 现价) / 近15日最高
                    current_close = df.iloc[-1]['close']
                    drawdown = (recent_15_high - current_close) / recent_15_high
                    
                    if drawdown < 0.10:
                        condition_met = True
                        strategy_name = f"涨停回调(涨停{limit_up_count}次,回调{drawdown:.1%})"

            # --- 条件2: 温和放量上涨策略 ---
            if not condition_met:
                # 1. 连续3个交易日收盘价格持续上涨
                recent_3 = df.iloc[-3:]
                if len(recent_3) == 3:
                    closes = recent_3['close'].values
                    is_rising_close = closes[0] < closes[1] < closes[2]
                    
                    # 2. 每日涨幅不超过5% (且大于0)
                    pcts = recent_3['pct_chg'].values
                    is_mild_rise = all(0 < p <= 5 for p in pcts)
                    
                    # 3. 交易量持续上升
                    vols = recent_3['volume'].values
                    is_rising_vol = vols[0] < vols[1] < vols[2]
                    
                    if is_rising_close and is_mild_rise and is_rising_vol:
                        condition_met = True
                        strategy_name = "温和放量三连阳"

            if not condition_met:
                return None
            
            # === 保存结果 ===
            # 获取股票名称和行业信息
            stock_info = self.db.get_stock_info(symbol=code)
            name = stock_info[0].name if stock_info else ""
            industry = stock_info[0].industry if stock_info else ""
            
            # 准备数据
            latest = df.iloc[-1]
            ma5 = latest.get('ma5', 0)
            ma10 = latest.get('ma10', 0)
            ma20 = latest.get('ma20', 0)
            
            strong_stock_data = {
                'code': code,
                'name': name,
                'last_price': str(latest['close']),
                'ma5': str(ma5),
                'ma10': str(ma10),
                'ma20': str(ma20),
                'industry': industry,
                'strategy_match': strategy_name,
                'market_value': market_value_yi,
                'pe_ratio': quote.pe_ratio
            }
            
            self.db.save_strong_stock(strong_stock_data)
            logger.info(f"发现强势股: {code} {name} - {strategy_name}")
                
            return code
        except Exception as e:
            logger.debug(f"分析 {code} 异常: {e}")
            return None


def save_tickers(tickers):
    """保存A股ticker列表到本地文件"""
    # 确保 reports 目录存在（使用项目根目录下的 reports）
    from pathlib import Path
    reports_dir = Path(__file__).parent.parent / 'resource/stock_name'
    reports_dir.mkdir(parents=True, exist_ok=True)
    TICKER_STORAGE_DIR = reports_dir
    ticker_file = TICKER_STORAGE_DIR / "cn_tickers.csv"
    df = pd.DataFrame(tickers, columns=['symbol', 'name'])
    df.to_csv(ticker_file, index=False)
    logger.info(f"已保存 {len(tickers)} 只ticker到 {ticker_file}")


def load_cached_tickers():
    """从本地文件加载已保存的A股ticker列表"""
    # 确保 reports 目录存在（使用项目根目录下的 reports）
    from pathlib import Path
    reports_dir = Path(__file__).parent.parent / 'resource/stock_name'
    reports_dir.mkdir(parents=True, exist_ok=True)
    TICKER_STORAGE_DIR = reports_dir
    ticker_file = TICKER_STORAGE_DIR / "cn_tickers.csv"
    if ticker_file.exists():
        try:
            df = pd.read_csv(ticker_file, dtype={'symbol': str})
            return [(str(row['symbol']).zfill(6), str(row['name'])) for _, row in df.iterrows()]
        except:
            return []
    return []

def fetch_all_cn_stocks():
    """获取所有A股股票列表（通过腾讯财经批量获取）"""
    import requests

    def get_stocks_batch_tencent(codes, market_prefix):
        """通过腾讯财经批量获取股票信息"""
        valid_stocks = []
        batch_size = 100  # 每次100个

        for i in range(0, len(codes), batch_size):
            batch = codes[i:i + batch_size]
            query_codes = ','.join([f"{market_prefix}{code}" for code in batch])
            url = f"http://qt.gtimg.cn/q={query_codes}"

            try:
                resp = requests.get(url, timeout=20)
                if resp.status_code == 200:
                    # 使用GBK解码
                    try:
                        text = resp.content.decode('gbk')
                    except:
                        text = resp.text

                    for line in text.strip().split(';'):
                        if '="' in line and '~' in line:
                            try:
                                parts = line.split('~')
                                if len(parts) >= 4:
                                    name = parts[1].strip()
                                    code = parts[2].strip()
                                    price_str = parts[3].strip()
                                    # 有名称、代码正确、价格>0
                                    if name and code and len(code) == 6:
                                        try:
                                            price = float(price_str)
                                            if price > 0:
                                                valid_stocks.append((code, name))
                                        except:
                                            pass
                            except:
                                continue
                time.sleep(0.02)
            except:
                continue

            if (i // batch_size) % 20 == 0 and i > 0:
                logger.info(f"   进度: {i}/{len(codes)}, 有效: {len(valid_stocks)} 只")

        return valid_stocks

    logger.info("📡 正在通过腾讯财经获取全部A股股票列表...")
    all_stocks = []

    # 上海A股代码范围（精简）
    logger.info("   🔍 获取上海A股...")
    sh_codes = []
    for i in range(600000, 606000):  # 主板 600000-605999
        sh_codes.append(str(i).zfill(6))
    for i in range(688000, 689500):  # 科创板 688000-689499
        sh_codes.append(str(i).zfill(6))

    sh_valid = get_stocks_batch_tencent(sh_codes, 'sh')
    logger.info(f"   ✅ 上海A股: {len(sh_valid)} 只")
    all_stocks.extend(sh_valid)

    # 深圳A股代码范围（精简）
    logger.info("   🔍 获取深圳A股...")
    sz_codes = []
    for i in range(1, 3500):  # 主板 000001-003499
        sz_codes.append(str(i).zfill(6))
    for i in range(300000, 302000):  # 创业板 300000-301999
        sz_codes.append(str(i).zfill(6))

    sz_valid = get_stocks_batch_tencent(sz_codes, 'sz')
    logger.info(f"   ✅ 深圳A股: {len(sz_valid)} 只")
    all_stocks.extend(sz_valid)

    if all_stocks:
        unique_stocks = list(dict.fromkeys(all_stocks))
        logger.info(f"✅ 成功获取 {len(unique_stocks)} 只A股")
        return unique_stocks

    # 备用：使用内置列表
    logger.warning("⚠️  腾讯财经获取失败，使用内置股票列表...")
    base_stocks = get_builtin_cn_stocks()
    logger.info(f"✅ 使用内置列表，共 {len(base_stocks)} 只A股")
    return base_stocks

def is_actual_cn_stock(symbol, name):
    """过滤掉ETF、基金、债券等非股票类型"""
    if not symbol or not name:
        return False

    symbol = str(symbol).upper()
    name = str(name).upper()

    # 过滤ETF和基金
    etf_keywords = ['ETF', '基金', '指数', 'LOF', '分级', '货币', '债券', 'FOF']
    for keyword in etf_keywords:
        if keyword in name:
            return False

    # 过滤ST股票（可选，取消注释可过滤ST）
    if 'ST' in name or '*ST' in name:
        return False
    
    # 过滤退市股票
    if '退' in name:
        return False

    # 过滤B股（代码以200、900开头）
    if symbol.startswith('200') or symbol.startswith('900'):
        return False

    # 过滤可转债、权证等
    if symbol.startswith('11') or symbol.startswith('12'):  # 可转债代码
        return False

    return True
def get_builtin_cn_stocks():
    """内置的基础A股列表（作为备用方案）"""
    # 包含主要的大中小盘股票代码
    stocks = [
        # 上证50主要成分股
        ("600000", "浦发银行"), ("600016", "民生银行"), ("600028", "中国石化"),
        ("600030", "中信证券"), ("600036", "招商银行"), ("600048", "保利发展"),
        ("600050", "中国联通"), ("600104", "上汽集团"), ("600111", "北方稀土"),
        ("600276", "恒瑞医药"), ("600309", "万华化学"), ("600519", "贵州茅台"),
        ("600585", "海螺水泥"), ("600690", "海尔智家"), ("600809", "山西汾酒"),
        ("600887", "伊利股份"), ("600900", "长江电力"), ("601012", "隆基绿能"),
        ("601088", "中国神华"), ("601166", "兴业银行"), ("601318", "中国平安"),
        ("601398", "工商银行"), ("601628", "中国人寿"), ("601668", "中国建筑"),
        ("601888", "中国中免"), ("601899", "紫金矿业"), ("603259", "药明康德"),
        ("603288", "海天味业"), ("603501", "韦尔股份"),

        # 深证成分股
        ("000001", "平安银行"), ("000002", "万科A"), ("000063", "中兴通讯"),
        ("000100", "TCL科技"), ("000157", "中联重科"), ("000333", "美的集团"),
        ("000338", "潍柴动力"), ("000425", "徐工机械"), ("000538", "云南白药"),
        ("000568", "泸州老窖"), ("000625", "长安汽车"), ("000651", "格力电器"),
        ("000661", "长春高新"), ("000725", "京东方A"), ("000776", "广发证券"),
        ("000858", "五粮液"), ("000895", "双汇发展"), ("000938", "紫光股份"),
        ("002001", "新和成"), ("002007", "华兰生物"), ("002027", "分众传媒"),
        ("002049", "紫光国微"), ("002050", "三花智控"), ("002120", "韵达股份"),
        ("002142", "宁波银行"), ("002230", "科大讯飞"), ("002236", "大华股份"),
        ("002241", "歌尔股份"), ("002271", "东方雨虹"), ("002304", "洋河股份"),
        ("002352", "顺丰控股"), ("002410", "广联达"), ("002415", "海康威视"),
        ("002460", "赣锋锂业"), ("002475", "立讯精密"), ("002594", "比亚迪"),
        ("002714", "牧原股份"), ("002812", "恩捷股份"), ("002916", "深南电路"),

        # 创业板股票
        ("300003", "乐普医疗"), ("300014", "亿纬锂能"), ("300015", "爱尔眼科"),
        ("300033", "同花顺"), ("300059", "东方财富"), ("300122", "智飞生物"),
        ("300124", "汇川技术"), ("300142", "沃森生物"), ("300146", "汤臣倍健"),
        ("300347", "泰格医药"), ("300408", "三环集团"), ("300413", "芒果超媒"),
        ("300433", "蓝思科技"), ("300450", "先导智能"), ("300454", "深信服"),
        ("300496", "中科创达"), ("300498", "温氏股份"), ("300529", "健帆生物"),
        ("300558", "贝达药业"), ("300595", "欧普康视"), ("300601", "康泰生物"),
        ("300628", "亿联网络"), ("300661", "圣邦股份"), ("300750", "宁德时代"),
        ("300760", "迈瑞医疗"), ("300782", "卓胜微"), ("300888", "稳健医疗"),
        ("300896", "爱美客"),

        # 科创板股票
        ("688005", "容百科技"), ("688009", "中国通号"), ("688012", "中微公司"),
        ("688036", "传音控股"), ("688111", "金山办公"), ("688126", "沪硅产业"),
        ("688169", "石头科技"), ("688180", "君实生物"), ("688187", "时代电气"),
        ("688188", "柏楚电子"), ("688200", "华峰测控"), ("688208", "道通科技"),
        ("688256", "寒武纪"), ("688269", "凯因科技"), ("688271", "联影医疗"),
        ("688303", "大全能源"), ("688363", "华熙生物"), ("688396", "华润微"),
        ("688516", "奥特维"), ("688520", "神州细胞"), ("688536", "思瑞浦"),
        ("688561", "奇安信"), ("688599", "天合光能"), ("688617", "惠泰医疗"),
        ("688658", "悦康药业"), ("688679", "通源环境"), ("688696", "极米科技"),
        ("688728", "格科微"), ("688772", "珠海冠宇"), ("688798", "艾为电子"),
        ("688799", "华纳药厂"), ("688819", "天能股份"),
    ]
    return stocks
if __name__ == "__main__":
    # 加载配置（在设置日志前加载，以获取日志目录）
    from src.config import get_config, Config
    config = get_config()
    from main import setup_logging
    # 配置日志（输出到控制台和文件）
    setup_logging()
    analyzer = StockAnalyzer()
    results = analyzer.scan_strong_stocks()
    print(results)

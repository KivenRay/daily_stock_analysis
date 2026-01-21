#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股强势股票扫描器
使用akshare获取A股数据，应用与美股相同的技术指标筛选逻辑
"""

import os
import logging
import pandas as pd
import datetime
import time
import random
import argparse
import json
from pathlib import Path
from sqlalchemy import Column, String, Float, Date, Integer, UniqueConstraint, DateTime
from sqlalchemy.orm import declarative_base

from storage import get_db, Base # 导入 Base

logger = logging.getLogger(__name__)
# ============ 清除代理设置（A股数据源不需要代理）============
# akshare 访问东方财富等国内数据源时，代理反而会导致 SSL 错误
for proxy_key in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'all_proxy']:
    if proxy_key in os.environ:
        del os.environ[proxy_key]
logger.info("🇨🇳 A股扫描器 - 已清除代理设置（国内数据源无需代理）")
# ================================================================
# 延时参数
REQUEST_DELAY_MIN = 0.1
REQUEST_DELAY_MAX = 0.3
BATCH_SIZE = 100
BATCH_PAUSE = 2

# API请求限制参数
MAX_RETRIES = 3
RETRY_BACKOFF = 2

# 路径配置 (相对于项目根目录)
ROOT_DIR = Path(__file__).parent # 修改 ROOT_DIR 为当前文件所在的目录
TICKER_STORAGE_DIR = ROOT_DIR / "ticker_storage"
TICKER_STORAGE_DIR.mkdir(exist_ok=True)
CACHE_DIR = ROOT_DIR / "cache_cn"
CACHE_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = ROOT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR = ROOT_DIR / "logs" # 新增 LOG_DIR
LOG_DIR.mkdir(exist_ok=True) # 确保 logs 目录存在
LOG_FILE_PATH = LOG_DIR / "strong_stocks_cn.log" # 修改日志文件路径

# 已退市股票过滤文件
DELISTED_STOCKS_FILE = TICKER_STORAGE_DIR / "delisted_stocks_cn.txt"

# SQLAlchemy ORM 基类 (已从 storage 导入，这里不再重复定义)
# Base = declarative_base()

class StrongStock(Base):
    """强势股票数据模型"""
    __tablename__ = 'strong_stocks'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(20), nullable=False, index=True, comment='股票代码')
    stock_name = Column(String(100), nullable=False, comment='股票名称')
    close_price = Column(Float, comment='收盘价')
    market_cap = Column(String(50), comment='市值(显示文本)')
    industry = Column(String(100), comment='行业')
    list_date = Column(String(20), comment='上市日期')
    ma5 = Column(Float, comment='MA5')
    ma10 = Column(Float, comment='MA10')
    ma20 = Column(Float, comment='MA20')
    macd = Column(Float, comment='MACD')
    macd_dea = Column(Float, comment='MACD_DEA')
    vol_ratio = Column(Float, comment='成交量倍数')
    increase_20d = Column(Float, comment='20天涨幅(%)')
    week_52_range = Column(String(50), comment='52周波动幅度')
    week_52_high = Column(Float, comment='52周最高')
    pct_from_high = Column(String(50), comment='距52周高点')
    week_52_low = Column(Float, comment='52周最低')
    pct_from_low = Column(String(50), comment='距52周低点')
    met_conditions = Column(String(50), comment='满足条件')
    condition_details = Column(String(500), comment='条件详情')
    scan_time = Column(DateTime, default=datetime.datetime.now, comment='扫描时间')
    
    __table_args__ = (
        UniqueConstraint('scan_time', 'stock_code', name='uix_strong_stock_scan_time_code'),
    )

def get_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='A股股票扫描工具')
    parser.add_argument('--use-cache', action='store_true', help='使用缓存的ticker列表（默认每次都获取全部A股）')
    parser.add_argument('--test', '-t', type=int, default=0, help='测试模式：只扫描前N只股票')
    parser.add_argument('--clear-cache', action='store_true', help='清除所有缓存的股票数据')
    parser.add_argument('--use-data-cache', action='store_true', help='使用股票数据缓存')
    parser.add_argument('--market', '-m', type=str, default='all',
                        choices=['all', 'sh', 'sz', 'bj', 'cyb', 'kcb'],
                        help='选择市场：all=全部, sh=上海主板, sz=深圳主板, bj=北交所, cyb=创业板, kcb=科创板')
    # 在包内运行时，直接解析可能会与 uvicorn 的参数冲突，这里我们用默认值
    # args = parser.parse_args()
    # return args.use_cache, args.test, args.clear_cache, args.use_data_cache, args.market
    return False, 0, False, True, 'all'


USE_TICKER_CACHE, TEST_LIMIT, CLEAR_CACHE, USE_DATA_CACHE, MARKET_FILTER = get_args()


def log_strong_stock(stock_info):
    """记录强势股票到日志文件"""
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] 🎯 检测到满足条件的A股: {stock_info['代码']} - {stock_info['名称']}\n"
    log_message += f"   💰 收盘价: ¥{stock_info['收盘价']}\n"
    log_message += f"   🏢 市值: {stock_info.get('市值', 'N/A')}\n"
    log_message += f"   📅 上市日期: {stock_info.get('上市日期', 'N/A')}\n"

    # 添加52周价格信息
    week_52_range = stock_info.get('52周波动幅度', 'N/A')
    week_52_high = stock_info.get('52周最高', 'N/A')
    week_52_low = stock_info.get('52周最低', 'N/A')
    pct_from_high = stock_info.get('距52周高点', 'N/A')
    pct_from_low = stock_info.get('距52周低点', 'N/A')
    if week_52_range != 'N/A':
        log_message += f"   📊 52周波动幅度: {week_52_range}\n"
    if week_52_high != 'N/A':
        log_message += f"   📊 52周最高: ¥{week_52_high} (当前距高点: {pct_from_high})\n"
    if week_52_low != 'N/A':
        log_message += f"   📊 52周最低: ¥{week_52_low} (当前距低点: {pct_from_low})\n"

    log_message += f"   📈 20天涨幅: {stock_info['20天涨幅']}%\n"
    log_message += f"   ⭐ 满足条件: {stock_info['满足条件']}\n"
    log_message += f"   📊 条件详情: {stock_info['条件详情']}\n"

    # 添加行业信息
    industry = stock_info.get('行业', 'N/A')
    if industry and industry != 'N/A':
        log_message += f"   🏭 行业: {industry}\n"

    log_message += f"   {'=' * 50}\n\n"

    # 写入日志文件
    try:
        with open(LOG_FILE_PATH, 'a', encoding='utf-8') as f:
            f.write(log_message)
    except Exception as e:
        logger.error(f"写入日志失败: {e}")

    # 同时打印到控制台
    logger.info(log_message.strip())


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


def load_cached_tickers():
    """从本地文件加载已保存的A股ticker列表"""
    ticker_file = TICKER_STORAGE_DIR / "cn_tickers.csv"
    if ticker_file.exists():
        try:
            df = pd.read_csv(ticker_file, dtype={'symbol': str})
            return [(str(row['symbol']).zfill(6), str(row['name'])) for _, row in df.iterrows()]
        except:
            return []
    return []


def save_tickers(tickers):
    """保存A股ticker列表到本地文件"""
    ticker_file = TICKER_STORAGE_DIR / "cn_tickers.csv"
    df = pd.DataFrame(tickers, columns=['symbol', 'name'])
    df.to_csv(ticker_file, index=False)
    logger.info(f"已保存 {len(tickers)} 只ticker到 {ticker_file}")


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
    # if 'ST' in name or '*ST' in name:
    #     return False

    # 过滤B股（代码以200、900开头）
    if symbol.startswith('200') or symbol.startswith('900'):
        return False

    # 过滤可转债、权证等
    if symbol.startswith('11') or symbol.startswith('12'):  # 可转债代码
        return False

    return True


def filter_by_market(stocks, market):
    """根据市场筛选股票"""
    if market == 'all':
        return stocks

    filtered = []
    for code, name in stocks:
        code = str(code).zfill(6)

        if market == 'sh':  # 上海主板（60开头）
            if code.startswith('60'):
                filtered.append((code, name))
        elif market == 'sz':  # 深圳主板（00开头）
            if code.startswith('00'):
                filtered.append((code, name))
        elif market == 'cyb':  # 创业板（30开头）
            if code.startswith('30'):
                filtered.append((code, name))
        elif market == 'kcb':  # 科创板（688开头）
            if code.startswith('688'):
                filtered.append((code, name))
        elif market == 'bj':  # 北交所（8开头或4开头）
            if code.startswith('8') or code.startswith('4'):
                filtered.append((code, name))

    return filtered


def get_all_stock_codes():
    """获取A股代码和名称（默认获取全部A股）"""
    if USE_TICKER_CACHE:
        # 使用缓存模式
        cached_tickers = load_cached_tickers()
        if cached_tickers:
            logger.info(f"📋 使用缓存的ticker列表，共 {len(cached_tickers)} 只股票")
            tickers = cached_tickers
        else:
            logger.warning("⚠️  未找到缓存的ticker列表，正在获取全部A股...")
            tickers = fetch_all_cn_stocks()
            save_tickers(tickers)
    else:
        # 默认模式：每次都获取全部A股
        logger.info("📡 正在获取全部A股股票列表...")
        tickers = fetch_all_cn_stocks()
        save_tickers(tickers)

    # 过滤ETF/基金等非股票
    logger.info("正在过滤ETF和非股票类型...")
    original_count = len(tickers)
    filtered_tickers = [(symbol, name) for symbol, name in tickers if is_actual_cn_stock(symbol, name)]
    filtered_count = original_count - len(filtered_tickers)
    logger.info(f"📊 过滤掉 {filtered_count} 只ETF/基金/债券等，剩余 {len(filtered_tickers)} 只纯股票")

    # 根据市场筛选
    if MARKET_FILTER != 'all':
        market_names = {
            'sh': '上海主板',
            'sz': '深圳主板',
            'cyb': '创业板',
            'kcb': '科创板',
            'bj': '北交所'
        }
        filtered_tickers = filter_by_market(filtered_tickers, MARKET_FILTER)
        logger.info(f"📊 筛选 {market_names.get(MARKET_FILTER, MARKET_FILTER)} 股票，共 {len(filtered_tickers)} 只")

    return filtered_tickers


def load_delisted_stocks():
    """加载已退市股票列表"""
    delisted = set()
    if DELISTED_STOCKS_FILE.exists():
        try:
            with open(DELISTED_STOCKS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    symbol = line.strip()
                    if symbol and not symbol.startswith('#'):
                        delisted.add(symbol)
            logger.info(f"📋 加载了 {len(delisted)} 只已退市股票过滤列表")
        except Exception as e:
            logger.error(f"⚠️  读取已退市股票列表失败: {e}")
    return delisted


def save_delisted_stock(symbol):
    """将股票代码添加到已退市股票列表"""
    try:
        existing = load_delisted_stocks()
        if symbol not in existing:
            with open(DELISTED_STOCKS_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{symbol}\n")
            logger.info(f"📝 已将 {symbol} 添加到已退市股票列表")
    except Exception as e:
        logger.error(f"⚠️  保存已退市股票失败: {e}")


def get_cache_filename(symbol):
    """获取股票数据缓存文件名"""
    return CACHE_DIR / f"{symbol}_data.json"


def load_cached_stock_data(symbol):
    """从缓存加载股票数据"""
    cache_file = get_cache_filename(symbol)
    if cache_file.exists():
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                cache_time = datetime.datetime.fromisoformat(data['cache_time'])
                if datetime.datetime.now() - cache_time < datetime.timedelta(days=1):
                    return data['stock_data']
        except Exception as e:
            logger.warning(f"⚠️  读取缓存失败 {symbol}: {e}")
    return None


def save_stock_data_to_cache(symbol, stock_data):
    """保存股票数据到缓存"""
    if stock_data is None:
        return

    cache_file = get_cache_filename(symbol)
    try:
        cache_data = {
            'cache_time': datetime.datetime.now().isoformat(),
            'stock_data': stock_data
        }
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"⚠️  保存缓存失败 {symbol}: {e}")


def clear_all_cache():
    """清除所有缓存的股票数据"""
    if CACHE_DIR.exists():
        import shutil
        try:
            shutil.rmtree(CACHE_DIR)
            CACHE_DIR.mkdir(exist_ok=True)
            logger.info("🗑️  已清除所有A股数据缓存")
        except Exception as e:
            logger.error(f"⚠️  清除缓存失败: {e}")
    else:
        logger.info("📭 缓存目录不存在，无需清除")


def fetch_stock_history_cn(symbol, days=90):
    """使用akshare获取A股历史行情"""
    try:
        import akshare as ak

        # 确保代码格式正确（6位数字）
        symbol = str(symbol).zfill(6)

        # 计算日期范围
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)

        # 使用东方财富数据源获取日K数据
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            adjust="qfq"  # 前复权
        )

        if df is None or df.empty:
            return None

        # 标准化列名
        df = df.rename(columns={
            '日期': '日期',
            '收盘': '收盘',
            '最高': '最高',
            '最低': '最低',
            '开盘': '开盘',
            '成交量': '成交量',
            '换手率': '换手率'
        })

        return df

    except Exception as e:
        error_str = str(e).lower()
        if 'delisted' in error_str or '退市' in error_str:
            save_delisted_stock(symbol)
        return None


def get_stock_info_cn(symbol):
    """获取A股股票基本信息"""
    try:
        import akshare as ak

        symbol = str(symbol).zfill(6)

        info = {
            'market_cap': None,
            'industry': 'N/A',
            'list_date': 'N/A',
            'fifty_two_week_high': 0,
            'fifty_two_week_low': 0
        }

        try:
            # 获取个股信息
            stock_info = ak.stock_individual_info_em(symbol=symbol)
            if stock_info is not None and not stock_info.empty:
                info_dict = dict(zip(stock_info['item'], stock_info['value']))

                # 总市值 - 东方财富返回的已经是元为单位
                if '总市值' in info_dict:
                    try:
                        market_cap_val = info_dict['总市值']
                        if isinstance(market_cap_val, (int, float)):
                            info['market_cap'] = float(market_cap_val)  # 已经是元为单位
                        else:
                            market_cap_str = str(market_cap_val)
                            market_cap_str = market_cap_str.replace(',', '').replace('亿', '')
                            info['market_cap'] = float(market_cap_str)
                    except:
                        pass

                # 行业
                if '行业' in info_dict:
                    info['industry'] = str(info_dict['行业'])

                # 上市日期
                if '上市时间' in info_dict:
                    info['list_date'] = str(info_dict['上市时间'])
        except:
            pass

        # 获取52周最高最低价
        try:
            # 获取一年的历史数据来计算52周高低价
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=365)

            df_year = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d'),
                adjust="qfq"
            )

            if df_year is not None and not df_year.empty:
                info['fifty_two_week_high'] = float(df_year['最高'].max())
                info['fifty_two_week_low'] = float(df_year['最低'].min())
        except:
            pass

        return info

    except Exception as e:
        return {
            'market_cap': None,
            'industry': 'N/A',
            'list_date': 'N/A',
            'fifty_two_week_high': 0,
            'fifty_two_week_low': 0
        }


def is_strong_stock(symbol, name, delisted_stocks=None):
    """使用综合技术指标判断股票强势程度"""
    # 检查是否在已退市列表中
    if delisted_stocks and symbol in delisted_stocks:
        return None

    try:
        # 使用数据缓存
        if USE_DATA_CACHE:
            cached_result = load_cached_stock_data(symbol)
            if cached_result:
                cache_date = cached_result.get('日期', '')
                today = datetime.datetime.now().strftime('%Y-%m-%d')
                if cache_date == today:
                    logger.info(f"📋 使用缓存数据: {symbol}")
                    return cached_result

        # 获取股票基本信息
        stock_info = get_stock_info_cn(symbol)
        market_cap = stock_info.get('market_cap')
        industry = stock_info.get('industry', 'N/A')
        list_date = stock_info.get('list_date', 'N/A')
        fifty_two_week_high = stock_info.get('fifty_two_week_high', 0)
        fifty_two_week_low = stock_info.get('fifty_two_week_low', 0)

        # 市值过滤：过滤超大盘股（>1万亿人民币）
        if market_cap:
            if market_cap > 1_000_000_000_000:  # 1万亿人民币
                market_cap_billions = market_cap / 100_000_000
                logger.info(f"🚫 过滤超大盘股: {symbol} (市值: {market_cap_billions:.0f}亿)")
                return None

        # 计算52周波动幅度
        week_52_range_pct = 0
        if fifty_two_week_low > 0 and fifty_two_week_high > 0:
            week_52_range_pct = ((fifty_two_week_high - fifty_two_week_low) / fifty_two_week_low) * 100
            # 过滤条件：52周波动幅度必须 >= 250%
            if week_52_range_pct < 250:
                logger.info(f"🚫 过滤波动幅度不足的股票: {symbol} (52周波动: {week_52_range_pct:.1f}% < 250%)")
                return None

        # 获取历史数据
        df = fetch_stock_history_cn(symbol, days=90)

        if df is None or df.empty or len(df) < 25:
            return None

        # 计算技术指标
        df['MA5'] = df['收盘'].rolling(5).mean()
        df['MA10'] = df['收盘'].rolling(10).mean()
        df['MA20'] = df['收盘'].rolling(20).mean()

        # MACD指标
        df['MACD_diff'] = df['收盘'].ewm(span=12).mean() - df['收盘'].ewm(span=26).mean()
        df['MACD_dea'] = df['MACD_diff'].ewm(span=9).mean()

        # 成交量均线
        df['VolMA5'] = df['成交量'].rolling(5).mean()

        # 20天涨幅
        df['RS_20d'] = df['收盘'].pct_change(periods=20)

        # 获取最新数据
        latest = df.iloc[-1]

        # 检查数据完整性
        required_fields = ['MA5', 'MA10', 'MA20', '收盘', 'MACD_diff', 'MACD_dea', '成交量', 'VolMA5']
        for field in required_fields:
            if field not in latest or pd.isna(latest[field]):
                return None

        # 获取数值
        ma5_val = float(latest['MA5'])
        ma10_val = float(latest['MA10'])
        ma20_val = float(latest['MA20'])
        close_val = float(latest['收盘'])
        macd_val = float(latest['MACD_diff'])
        dea_val = float(latest['MACD_dea'])
        vol_val = float(latest['成交量'])
        vol_ma5_val = float(latest['VolMA5'])

        # 趋势强度判断
        conditions = {
            '短期趋势': ma5_val > ma10_val,
            '中期趋势': ma10_val > ma20_val,
            '价格强势': close_val > ma5_val,
            'MACD信号': macd_val > dea_val and macd_val > 0,
            '成交量': vol_val > vol_ma5_val * 0.5,
        }

        # 20天涨幅条件
        rs_20d_float = 0
        if 'RS_20d' in latest.index and not pd.isna(latest['RS_20d']):
            rs_20d_float = float(latest['RS_20d'])
            conditions['相对强度'] = rs_20d_float > 0.15

        # 计算满足条件的数量
        met_conditions = sum(conditions.values())
        total_conditions = len(conditions)

        # 只有满足所有6个条件才写入
        if met_conditions == total_conditions and total_conditions == 6:
            date_str = str(latest['日期'])[:10] if '日期' in latest.index else datetime.datetime.now().strftime(
                '%Y-%m-%d')

            # 格式化市值显示
            market_cap_display = "N/A"
            if market_cap:
                if market_cap >= 100_000_000_000:  # 1000亿以上
                    market_cap_display = f"¥{market_cap / 100_000_000:.0f}亿"
                elif market_cap >= 100_000_000:  # 1亿以上
                    market_cap_display = f"¥{market_cap / 100_000_000:.2f}亿"
                else:
                    market_cap_display = f"¥{market_cap:,.0f}"

            # 计算距52周高低价的百分比
            pct_from_high = 0
            pct_from_low = 0
            if fifty_two_week_high > 0:
                pct_from_high = ((close_val - fifty_two_week_high) / fifty_two_week_high) * 100
            if fifty_two_week_low > 0:
                pct_from_low = ((close_val - fifty_two_week_low) / fifty_two_week_low) * 100

            result = {
                "代码": symbol,
                "名称": name,
                "52周波动幅度": f"{round(week_52_range_pct, 2)}%" if week_52_range_pct > 0 else "N/A",
                "52周最高": round(fifty_two_week_high, 2) if fifty_two_week_high > 0 else "N/A",
                "距52周高点": f"{round(pct_from_high, 2)}%" if fifty_two_week_high > 0 else "N/A",
                "52周最低": round(fifty_two_week_low, 2) if fifty_two_week_low > 0 else "N/A",
                "距52周低点": f"{round(pct_from_low, 2)}%" if fifty_two_week_low > 0 else "N/A",
                "上市日期": list_date,
                "收盘价": round(close_val, 2),
                "市值": market_cap_display,
                "行业": industry,
                "MA5": round(ma5_val, 2),
                "MA10": round(ma10_val, 2),
                "MA20": round(ma20_val, 2),
                "MACD": round(macd_val, 4),
                "MACD_DEA": round(dea_val, 4),
                "成交量倍数": round(vol_val / vol_ma5_val, 2) if vol_ma5_val > 0 else 0,
                "20天涨幅": round(rs_20d_float * 100, 2),
                "满足条件": f"{met_conditions}/{total_conditions}",
                "条件详情": '|'.join([k for k, v in conditions.items() if v])
            }

            if USE_DATA_CACHE:
                save_stock_data_to_cache(symbol, result)
            return result
        else:
            if USE_DATA_CACHE:
                negative_result = {
                    "代码": symbol,
                    "名称": name,
                    "日期": datetime.datetime.now().strftime('%Y-%m-%d'),
                    "不符合条件": True,
                    "满足条件": f"{met_conditions}/{total_conditions}"
                }
                save_stock_data_to_cache(symbol, negative_result)
            return None

    except Exception as e:
        error_str = str(e).lower()
        if any(keyword in error_str for keyword in ['delisted', '退市', 'no data', 'not found']):
            if delisted_stocks is not None:
                save_delisted_stock(symbol)
        return None


def get_output_filename():
    """生成带日期后缀的文件名"""
    now = datetime.datetime.now()
    return str(OUTPUT_DIR / f"strong_stocks_cn_{now.strftime('%Y%m%d_%H')}.xlsx")


def save_strong_stocks_to_db(df: pd.DataFrame):
    """将强势股票扫描结果保存到数据库"""
    if df.empty:
        logger.info("没有扫描到强势股票，无需保存到数据库。")
        return

    db = get_db()
    # 确保 strong_stocks 表已创建
    Base.metadata.create_all(db._engine)
    
    with db.get_session() as session:
        try:
            for _, row in df.iterrows():
                stock = StrongStock(
                    scan_time=datetime.datetime.now(), # 记录当前扫描时间
                    stock_code=row['代码'],
                    stock_name=row['名称'],
                    close_price=row['收盘价'],
                    market_cap=row['市值'],
                    industry=row['行业'],
                    list_date=row['上市日期'],
                    ma5=row['MA5'],
                    ma10=row['MA10'],
                    ma20=row['MA20'],
                    macd=row['MACD'],
                    macd_dea=row['MACD_DEA'],
                    vol_ratio=row['成交量倍数'],
                    increase_20d=row['20天涨幅'],
                    week_52_range=row['52周波动幅度'],
                    week_52_high=row['52周最高'] if row['52周最高'] != 'N/A' else None,
                    pct_from_high=row['距52周高点'],
                    week_52_low=row['52周最低'] if row['52周最低'] != 'N/A' else None,
                    pct_from_low=row['距52周低点'],
                    met_conditions=row['满足条件'],
                    condition_details=row['条件详情']
                )
                session.merge(stock) # 使用 merge 进行 UPSERT 操作
            session.commit()
            logger.info(f"成功将 {len(df)} 条强势股票扫描结果保存到数据库。")
        except Exception as e:
            session.rollback()
            logger.error(f"保存强势股票扫描结果到数据库时出错: {e}", exc_info=True)


def scan_market():
    """扫描A股市场"""
    start_time = datetime.datetime.now()
    logger.info(f"\n{'=' * 60}")
    logger.info(f"开始扫描A股强势股票...")
    logger.info(f"扫描条件: MA5>MA10 + MA10>MA20 + 价格>MA5 + MACD金叉为正 + 成交量不过度萎缩 + 20天涨幅>15%")
    logger.info(f"排序规则: 按20天涨幅从小到大排序")
    logger.info(f"过滤条件: 排除ETF/基金/债券 + 市值<1万亿 + 52周波动幅度>=250%")
    logger.info(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'=' * 60}\n")

    if CLEAR_CACHE:
        clear_all_cache()

    delisted_stocks = load_delisted_stocks()
    codes = get_all_stock_codes()

    # 过滤已退市股票
    if delisted_stocks:
        original_count = len(codes)
        codes = [(code, name) for code, name in codes if code not in delisted_stocks]
        filtered_count = original_count - len(codes)
        if filtered_count > 0:
            logger.info(f"🚫 已过滤 {filtered_count} 只已退市股票\n")

    # 测试模式
    if TEST_LIMIT > 0:
        codes = codes[:TEST_LIMIT]
        logger.warning(f"⚠️  测试模式：只扫描前 {TEST_LIMIT} 只股票\n")

    logger.info(f"📊 总共需要扫描 {len(codes)} 只股票")
    cache_strategy = "使用股票数据缓存" if USE_DATA_CACHE else "实时获取数据"
    logger.info(f"💾 缓存策略：{cache_strategy}\n")

    results = []
    skipped = []
    output_file = get_output_filename()

    for idx, (code, name) in enumerate(codes, 1):
        code_str = str(code).zfill(6)
        name_str = str(name)
        logger.info(f"[{idx:4d}/{len(codes)}] 🔍 扫描: {code_str} - {name_str[:20]}")

        try:
            res = is_strong_stock(code_str, name_str, delisted_stocks)
            if res:
                results.append(res)

                # 按20天涨幅排序
                def sort_key(x):
                    return x.get('20天涨幅', 0)

                results.sort(key=sort_key)
                log_strong_stock(res)
                logger.info(f"✅ 找到强势股票！当前共 {len(results)} 只：")
                logger.info(f"   {code_str}: 收盘价=¥{res['收盘价']}, 满足{res['满足条件']}条件, 20天涨幅{res['20天涨幅']}%")
        except Exception as e:
            logger.error(f"❌ 扫描 {code_str} 时出错: {e}", exc_info=True)
            skipped.append((code_str, name_str))

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        if idx % BATCH_SIZE == 0:
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            logger.info(f"\n⏸️  已扫描 {idx} 只，休息 {BATCH_PAUSE} 秒...")
            logger.info(f"   已用时: {elapsed:.1f} 秒，进度: {idx / len(codes) * 100:.1f}%")
            if results:
                logger.info(f"   📈 当前强势股票前3名:")
                for i, top_stock in enumerate(results[:3], 1):
                    logger.info(f"     {i}. {top_stock['代码']} {top_stock['名称'][:10]}")
            time.sleep(BATCH_PAUSE)
            logger.info("")

    end_time = datetime.datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()

    # 最终排序
    def sort_key(x):
        return x.get('20天涨幅', 0)

    results.sort(key=sort_key)
    df = pd.DataFrame(results)

    logger.info(f"\n{'=' * 60}")
    logger.info(f"扫描完成！")
    logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"总用时: {elapsed_time:.1f} 秒 ({elapsed_time / 60:.1f} 分钟)")
    logger.info(f"扫描股票数: {len(codes)}")
    logger.info(f"符合条件的股票: {len(results)} 只")
    logger.info(f"跳过的股票: {len(skipped)} 只")
    logger.info(f"{'=' * 60}\n")

    if not df.empty:
        try:
            df.to_excel(output_file, index=False)
            logger.info(f"📊 结果已保存到: {output_file}")
            save_strong_stocks_to_db(df) # 调用保存到数据库的函数
        except PermissionError:
            logger.error(f"❌ 无法写入 {output_file}，请关闭 Excel 文件后重试。")

    return df

if __name__ == "__main__":
    # 检查akshare是否安装
    try:
        import akshare as ak

        logger.info(f"✅ akshare 版本: {ak.__version__}")
    except ImportError:
        logger.error("❌ 请先安装akshare库:")
        logger.error("   pip install akshare")
        logger.error("   或者: ./venv/bin/pip install akshare")
        exit(1)

    result_df = scan_market()
    if not result_df.empty:
        logger.info("\n📋 扫描结果详情（按20天涨幅从小到大排序）：")
        logger.info("=" * 80)
        logger.info(f"\n{result_df.to_string(index=False)}")
        logger.info("=" * 80)
        # output_file 变量在 scan_market 内部定义，这里无法直接访问，所以注释掉
        # logger.info(f"\n✅ 结果已保存到: {output_file}")
        logger.info(f"📁 共 {len(result_df)} 只符合条件的A股\n")
    else:
        logger.warning("\n⚠️  今天没有找到满足全部6个条件的A股强势股票。\n")

# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 存储层
===================================

职责：
1. 管理 SQLite 数据库连接（单例模式）
2. 定义 ORM 数据模型
3. 提供数据存取接口
4. 实现智能更新逻辑（断点续传）
"""

import atexit
import hashlib
import json
import logging
import re
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, TYPE_CHECKING, Union, Tuple
from pathlib import Path

import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Date,
    DateTime,
    Integer,
    Index,
    UniqueConstraint,
    Text,
    select,
    and_,
    desc,
    func,
    Text,
)
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)
from sqlalchemy.exc import IntegrityError

from src.config import get_config

logger = logging.getLogger(__name__)

# SQLAlchemy ORM 基类
Base = declarative_base()

if TYPE_CHECKING:
    from src.search_service import SearchResponse


# === 数据模型定义 ===

class StockDaily(Base):
    """
    股票日线数据模型
    
    存储每日行情数据和计算的技术指标
    支持多股票、多日期的唯一约束
    """
    __tablename__ = 'stock_daily'
    
    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # 股票代码（如 600519, 000001）
    code = Column(String(10), nullable=False, index=True)
    
    # 交易日期
    date = Column(Date, nullable=False, index=True)
    
    # OHLC 数据
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    
    # 成交数据
    volume = Column(Float)  # 成交量（股）
    amount = Column(Float)  # 成交额（元）
    pct_chg = Column(Float)  # 涨跌幅（%）
    
    # 技术指标
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma20 = Column(Float)
    volume_ratio = Column(Float)  # 量比
    
    # 数据来源
    data_source = Column(String(50))  # 记录数据来源（如 AkshareFetcher）
    
    # 更新时间
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 唯一约束：同一股票同一日期只能有一条数据
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date'),
    )
    
    def __repr__(self):
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }

class NewsIntel(Base):
    """
    新闻情报数据模型

    存储搜索到的新闻情报条目，用于后续分析与查询
    """
    __tablename__ = 'news_intel'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联用户查询操作
    query_id = Column(String(64), index=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # 搜索上下文
    dimension = Column(String(32), index=True)  # latest_news / risk_check / earnings / market_analysis / industry
    query = Column(String(255))
    provider = Column(String(32), index=True)

    # 新闻内容
    title = Column(String(300), nullable=False)
    snippet = Column(Text)
    url = Column(String(1000), nullable=False)
    source = Column(String(100))
    published_date = Column(DateTime, index=True)

    # 入库时间
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    query_source = Column(String(32), index=True)  # bot/web/cli/system
    requester_platform = Column(String(20))
    requester_user_id = Column(String(64))
    requester_user_name = Column(String(64))
    requester_chat_id = Column(String(64))
    requester_message_id = Column(String(64))
    requester_query = Column(String(255))

    __table_args__ = (
        UniqueConstraint('url', name='uix_news_url'),
        Index('ix_news_code_pub', 'code', 'published_date'),
    )

    def __repr__(self) -> str:
        return f"<NewsIntel(code={self.code}, title={self.title[:20]}...)>"


class AnalysisHistory(Base):
    """
    分析结果历史记录模型

    保存每次分析结果，支持按 query_id/股票代码检索
    """
    __tablename__ = 'analysis_history'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联查询链路
    query_id = Column(String(64), index=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    report_type = Column(String(16), index=True)

    # 核心结论
    sentiment_score = Column(Integer)
    operation_advice = Column(String(20))
    trend_prediction = Column(String(50))
    analysis_summary = Column(Text)

    # 详细数据
    raw_result = Column(Text)
    news_content = Column(Text)
    context_snapshot = Column(Text)

    # 狙击点位（用于回测）
    ideal_buy = Column(Float)
    secondary_buy = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)

    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_analysis_code_time', 'code', 'created_at'),
    )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'query_id': self.query_id,
            'code': self.code,
            'name': self.name,
            'report_type': self.report_type,
            'sentiment_score': self.sentiment_score,
            'operation_advice': self.operation_advice,
            'trend_prediction': self.trend_prediction,
            'analysis_summary': self.analysis_summary,
            'raw_result': self.raw_result,
            'news_content': self.news_content,
            'context_snapshot': self.context_snapshot,
            'ideal_buy': self.ideal_buy,
            'secondary_buy': self.secondary_buy,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class StockInfo(Base):
    """
    股票基础信息表

    对应 SQL:
    CREATE TABLE stock_info (
        id INTEGER NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        symbol VARCHAR(10),
        name VARCHAR(50),
        area VARCHAR(50),
        industry VARCHAR(50),
        fullname VARCHAR(100),
        enname VARCHAR(100),
        cnspell VARCHAR(50),
        market VARCHAR(20),
        exchange VARCHAR(20),
        curr_type VARCHAR(20),
        list_status VARCHAR(10),
        list_date VARCHAR(20),
        delist_date VARCHAR(20),
        is_hs VARCHAR(10),
        act_name VARCHAR(100),
        act_ent_type VARCHAR(100),
        created_at DATETIME,
        updated_at DATETIME,
        PRIMARY KEY (id),
        UNIQUE (ts_code)
    );
    """
    __tablename__ = 'stock_info'

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 基础信息
    ts_code = Column(String(20), unique=True, index=True, nullable=False)  # TS代码 (唯一约束)
    symbol = Column(String(10), index=True)  # 股票代码
    name = Column(String(50))  # 股票名称
    area = Column(String(50))  # 地域
    industry = Column(String(50))  # 所属行业
    fullname = Column(String(100))  # 股票全称
    enname = Column(String(100))  # 英文全称
    cnspell = Column(String(50))  # 拼音缩写
    market = Column(String(20))  # 市场类型
    exchange = Column(String(20))  # 交易所代码
    curr_type = Column(String(20))  # 交易货币
    list_status = Column(String(10))  # 上市状态 L上市 D退市 P暂停上市
    list_date = Column(String(20))  # 上市日期
    delist_date = Column(String(20))  # 退市日期
    is_hs = Column(String(10))  # 是否沪深港通标的，N否 H沪股通 S深股通
    act_name = Column(String(100))  # 实控人名称
    act_ent_type = Column(String(100))  # 实控人企业性质

    # 更新时间
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<StockInfo(ts_code={self.ts_code}, name={self.name})>"

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'ts_code': self.ts_code,
            'symbol': self.symbol,
            'name': self.name,
            'area': self.area,
            'industry': self.industry,
            'fullname': self.fullname,
            'enname': self.enname,
            'cnspell': self.cnspell,
            'market': self.market,
            'exchange': self.exchange,
            'curr_type': self.curr_type,
            'list_status': self.list_status,
            'list_date': self.list_date,
            'delist_date': self.delist_date,
            'is_hs': self.is_hs,
            'act_name': self.act_name,
            'act_ent_type': self.act_ent_type,
        }


class StrongStockInfo(Base):
    """
    强势股信息表

    存储筛选出的强势股信息
    """
    __tablename__ = 'strong_stock_info'

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码（唯一主键）
    code = Column(String(10), unique=True, nullable=False, index=True)

    # 股票名称
    name = Column(String(50))

    # 最新价格
    last_price = Column(String(20))

    # 均线数据
    ma5 = Column(String(20))
    ma10 = Column(String(20))
    ma20 = Column(String(20))

    # 所属行业
    industry = Column(String(50))

    # AI 分析结果
    ai_analysis = Column(Text)
    
    # 策略匹配
    strategy_match = Column(String(500))
    
    # 市值（亿）
    market_value = Column(Float)
    
    # 市盈率
    pe_ratio = Column(Float)
    
    # 筛选日期
    date = Column(Date, default=date.today)

    # 更新时间
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<StrongStockInfo(code={self.code}, name={self.name})>"


class DatabaseManager:
    """
    数据库管理器 - 单例模式
    
    职责：
    1. 管理数据库连接池
    2. 提供 Session 上下文管理
    3. 封装数据存取操作
    """
    
    _instance: Optional['DatabaseManager'] = None
    _initialized: bool = False

    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url: Optional[str] = None):
        """
        初始化数据库管理器
        
        Args:
            db_url: 数据库连接 URL（可选，默认从配置读取）
        """
        if getattr(self, '_initialized', False):
            return
        
        if db_url is None:
            config = get_config()
            db_url = config.get_db_url()
        
        # 创建数据库引擎
        self._engine = create_engine(
            db_url,
            echo=False,  # 设为 True 可查看 SQL 语句
            pool_pre_ping=True,  # 连接健康检查
        )
        
        # 创建 Session 工厂
        self._SessionLocal = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
        )
        
        # 创建所有表
        # 注意：SQLAlchemy 会自动生成 CREATE TABLE 语句并执行
        Base.metadata.create_all(self._engine)

        self._initialized = True
        logger.info(f"数据库初始化完成: {db_url}")

        # 注册退出钩子，确保程序退出时关闭数据库连接
        atexit.register(DatabaseManager._cleanup_engine, self._engine)
    
    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（用于测试）"""
        if cls._instance is not None:
            if hasattr(cls._instance, '_engine') and cls._instance._engine is not None:
                cls._instance._engine.dispose()
            cls._instance._initialized = False
            cls._instance = None

    @classmethod
    def _cleanup_engine(cls, engine) -> None:
        """
        清理数据库引擎（atexit 钩子）

        确保程序退出时关闭所有数据库连接，避免 ResourceWarning

        Args:
            engine: SQLAlchemy 引擎对象
        """
        try:
            if engine is not None:
                engine.dispose()
                logger.debug("数据库引擎已清理")
        except Exception as e:
            logger.warning(f"清理数据库引擎时出错: {e}")
    
    def get_session(self) -> Session:
        """
        获取数据库 Session
        
        使用示例:
            with db.get_session() as session:
                # 执行查询
                session.commit()  # 如果需要
        """
        if not getattr(self, '_initialized', False) or not hasattr(self, '_SessionLocal'):
            raise RuntimeError(
                "DatabaseManager 未正确初始化。"
                "请确保通过 DatabaseManager.get_instance() 获取实例。"
            )
        session = self._SessionLocal()
        try:
            return session
        except Exception:
            session.close()
            raise
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        检查是否已有指定日期的数据
        
        用于断点续传逻辑：如果已有数据则跳过网络请求
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            是否存在数据
        """
        if target_date is None:
            target_date = date.today()
        
        with self.get_session() as session:
            result = session.execute(
                select(StockDaily).where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date == target_date
                    )
                )
            ).scalar_one_or_none()
            
            return result is not None
    
    def get_latest_data(
        self, 
        code: str, 
        days: int = 2
    ) -> List[StockDaily]:
        """
        获取最近 N 天的数据
        
        用于计算"相比昨日"的变化
        
        Args:
            code: 股票代码
            days: 获取天数
            
        Returns:
            StockDaily 对象列表（按日期降序）
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()
            
            return list(results)

    def save_news_intel(
        self,
        code: str,
        name: str,
        dimension: str,
        query: str,
        response: 'SearchResponse',
        query_context: Optional[Dict[str, str]] = None
    ) -> int:
        """
        保存新闻情报到数据库

        去重策略：
        - 优先按 URL 去重（唯一约束）
        - URL 缺失时按 title + source + published_date 进行软去重

        关联策略：
        - query_context 记录用户查询信息（平台、用户、会话、原始指令等）
        """
        if not response or not response.results:
            return 0

        saved_count = 0

        with self.get_session() as session:
            try:
                for item in response.results:
                    title = (item.title or '').strip()
                    url = (item.url or '').strip()
                    source = (item.source or '').strip()
                    snippet = (item.snippet or '').strip()
                    published_date = self._parse_published_date(item.published_date)

                    if not title and not url:
                        continue

                    url_key = url or self._build_fallback_url_key(
                        code=code,
                        title=title,
                        source=source,
                        published_date=published_date
                    )

                    # 优先按 URL 或兜底键去重
                    existing = session.execute(
                        select(NewsIntel).where(NewsIntel.url == url_key)
                    ).scalar_one_or_none()

                    if existing:
                        existing.name = name or existing.name
                        existing.dimension = dimension or existing.dimension
                        existing.query = query or existing.query
                        existing.provider = response.provider or existing.provider
                        existing.snippet = snippet or existing.snippet
                        existing.source = source or existing.source
                        existing.published_date = published_date or existing.published_date
                        existing.fetched_at = datetime.now()

                        if query_context:
                            existing.query_id = query_context.get("query_id") or existing.query_id
                            existing.query_source = query_context.get("query_source") or existing.query_source
                            existing.requester_platform = query_context.get("requester_platform") or existing.requester_platform
                            existing.requester_user_id = query_context.get("requester_user_id") or existing.requester_user_id
                            existing.requester_user_name = query_context.get("requester_user_name") or existing.requester_user_name
                            existing.requester_chat_id = query_context.get("requester_chat_id") or existing.requester_chat_id
                            existing.requester_message_id = query_context.get("requester_message_id") or existing.requester_message_id
                            existing.requester_query = query_context.get("requester_query") or existing.requester_query
                    else:
                        try:
                            with session.begin_nested():
                                record = NewsIntel(
                                    code=code,
                                    name=name,
                                    dimension=dimension,
                                    query=query,
                                    provider=response.provider,
                                    title=title,
                                    snippet=snippet,
                                    url=url_key,
                                    source=source,
                                    published_date=published_date,
                                    fetched_at=datetime.now(),
                                    query_id=(query_context or {}).get("query_id"),
                                    query_source=(query_context or {}).get("query_source"),
                                    requester_platform=(query_context or {}).get("requester_platform"),
                                    requester_user_id=(query_context or {}).get("requester_user_id"),
                                    requester_user_name=(query_context or {}).get("requester_user_name"),
                                    requester_chat_id=(query_context or {}).get("requester_chat_id"),
                                    requester_message_id=(query_context or {}).get("requester_message_id"),
                                    requester_query=(query_context or {}).get("requester_query"),
                                )
                                session.add(record)
                                session.flush()
                            saved_count += 1
                        except IntegrityError:
                            # 单条 URL 唯一约束冲突（如并发插入），仅跳过本条，保留本批其余成功项
                            logger.debug("新闻情报重复（已跳过）: %s %s", code, url_key)

                session.commit()
                logger.info(f"保存新闻情报成功: {code}, 新增 {saved_count} 条")

            except Exception as e:
                session.rollback()
                logger.error(f"保存新闻情报失败: {e}")
                raise

        return saved_count

    def get_recent_news(self, code: str, days: int = 7, limit: int = 20) -> List[NewsIntel]:
        """
        获取指定股票最近 N 天的新闻情报
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(
                    and_(
                        NewsIntel.code == code,
                        NewsIntel.fetched_at >= cutoff_date
                    )
                )
                .order_by(desc(NewsIntel.fetched_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_news_intel_by_query_id(self, query_id: str, limit: int = 20) -> List[NewsIntel]:
        """
        根据 query_id 获取新闻情报列表

        Args:
            query_id: 分析记录唯一标识
            limit: 返回数量限制

        Returns:
            NewsIntel 列表（按发布时间或抓取时间倒序）
        """
        from sqlalchemy import func

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(NewsIntel.query_id == query_id)
                .order_by(
                    desc(func.coalesce(NewsIntel.published_date, NewsIntel.fetched_at)),
                    desc(NewsIntel.fetched_at)
                )
                .limit(limit)
            ).scalars().all()

            return list(results)

    def save_analysis_history(
        self,
        result: Any,
        query_id: str,
        report_type: str,
        news_content: Optional[str],
        context_snapshot: Optional[Dict[str, Any]] = None,
        save_snapshot: bool = True
    ) -> int:
        """
        保存分析结果历史记录
        """
        if result is None:
            return 0

        sniper_points = self._extract_sniper_points(result)
        raw_result = self._build_raw_result(result)
        context_text = None
        if save_snapshot and context_snapshot is not None:
            context_text = self._safe_json_dumps(context_snapshot)

        record = AnalysisHistory(
            query_id=query_id,
            code=result.code,
            name=result.name,
            report_type=report_type,
            sentiment_score=result.sentiment_score,
            operation_advice=result.operation_advice,
            trend_prediction=result.trend_prediction,
            analysis_summary=result.analysis_summary,
            raw_result=self._safe_json_dumps(raw_result),
            news_content=news_content,
            context_snapshot=context_text,
            ideal_buy=sniper_points.get("ideal_buy"),
            secondary_buy=sniper_points.get("secondary_buy"),
            stop_loss=sniper_points.get("stop_loss"),
            take_profit=sniper_points.get("take_profit"),
            created_at=datetime.now(),
        )

        with self.get_session() as session:
            try:
                session.add(record)
                session.commit()
                return 1
            except Exception as e:
                session.rollback()
                logger.error(f"保存分析历史失败: {e}")
                return 0

    def get_analysis_history(
        self,
        code: Optional[str] = None,
        query_id: Optional[str] = None,
        days: int = 30,
        limit: int = 50
    ) -> List[AnalysisHistory]:
        """
        查询分析历史记录
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            conditions = [AnalysisHistory.created_at >= cutoff_date]
            if code:
                conditions.append(AnalysisHistory.code == code)
            if query_id:
                conditions.append(AnalysisHistory.query_id == query_id)

            results = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(limit)
            ).scalars().all()

            return list(results)
    
    def get_analysis_history_paginated(
        self,
        code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[AnalysisHistory], int]:
        """
        分页查询分析历史记录（带总数）

        Args:
            code: 股票代码筛选
            start_date: 开始日期（含）
            end_date: 结束日期（含）
            offset: 偏移量（跳过前 N 条）
            limit: 每页数量

        Returns:
            Tuple[List[AnalysisHistory], int]: (记录列表, 总数)
        """
        from sqlalchemy import func

        with self.get_session() as session:
            conditions = []

            if code:
                conditions.append(AnalysisHistory.code == code)
            if start_date:
                # created_at >= start_date 00:00:00
                conditions.append(AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time()))
            if end_date:
                # created_at < end_date+1 00:00:00 (即 <= end_date 23:59:59)
                conditions.append(AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))

            # 构建 where 子句
            where_clause = and_(*conditions) if conditions else True

            # 查询总数
            total_query = select(func.count(AnalysisHistory.id)).where(where_clause)
            total = session.execute(total_query).scalar() or 0

            # 查询分页数据
            data_query = (
                select(AnalysisHistory)
                .where(where_clause)
                .order_by(desc(AnalysisHistory.created_at))
                .offset(offset)
                .limit(limit)
            )
            results = session.execute(data_query).scalars().all()

            return list(results), total

    def get_data_range(
        self, 
        code: str, 
        start_date: date, 
        end_date: date
    ) -> List[StockDaily]:
        """
        获取指定日期范围的数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            StockDaily 对象列表
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(StockDaily.date)
            ).scalars().all()
            
            return list(results)
    
    def save_daily_data(
        self, 
        df: pd.DataFrame, 
        code: str,
        data_source: str = "Unknown"
    ) -> int:
        """
        保存日线数据到数据库
        
        策略：
        - 使用 UPSERT 逻辑（存在则更新，不存在则插入）
        - 跳过已存在的数据，避免重复
        
        Args:
            df: 包含日线数据的 DataFrame
            code: 股票代码
            data_source: 数据来源名称
            
        Returns:
            新增/更新的记录数
        """
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0
        
        saved_count = 0
        
        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    # 解析日期
                    row_date = row.get('date')
                    if isinstance(row_date, str):
                        row_date = datetime.strptime(row_date, '%Y-%m-%d').date()
                    elif isinstance(row_date, datetime):
                        row_date = row_date.date()
                    elif isinstance(row_date, pd.Timestamp):
                        row_date = row_date.date()
                    
                    # 检查是否已存在
                    existing = session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == code,
                                StockDaily.date == row_date
                            )
                        )
                    ).scalar_one_or_none()
                    
                    if existing:
                        # 更新现有记录
                        existing.open = row.get('open')
                        existing.high = row.get('high')
                        existing.low = row.get('low')
                        existing.close = row.get('close')
                        existing.volume = row.get('volume')
                        existing.amount = row.get('amount')
                        existing.pct_chg = row.get('pct_chg')
                        existing.ma5 = row.get('ma5')
                        existing.ma10 = row.get('ma10')
                        existing.ma20 = row.get('ma20')
                        existing.volume_ratio = row.get('volume_ratio')
                        existing.data_source = data_source
                        existing.updated_at = datetime.now()
                    else:
                        # 创建新记录
                        record = StockDaily(
                            code=code,
                            date=row_date,
                            open=row.get('open'),
                            high=row.get('high'),
                            low=row.get('low'),
                            close=row.get('close'),
                            volume=row.get('volume'),
                            amount=row.get('amount'),
                            pct_chg=row.get('pct_chg'),
                            ma5=row.get('ma5'),
                            ma10=row.get('ma10'),
                            ma20=row.get('ma20'),
                            volume_ratio=row.get('volume_ratio'),
                            data_source=data_source,
                        )
                        session.add(record)
                        saved_count += 1
                
                session.commit()
                logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条")
                
            except Exception as e:
                session.rollback()
                logger.error(f"保存 {code} 数据失败: {e}")
                raise
        
        return saved_count
    
    def get_analysis_context(
        self, 
        code: str,
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取分析所需的上下文数据
        
        返回今日数据 + 昨日数据的对比信息
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            包含今日数据、昨日对比等信息的字典
        """
        if target_date is None:
            target_date = date.today()
        
        # 获取最近2天数据
        recent_data = self.get_latest_data(code, days=2)
        
        if not recent_data:
            logger.warning(f"未找到 {code} 的数据")
            return None
        
        today_data = recent_data[0]
        yesterday_data = recent_data[1] if len(recent_data) > 1 else None
        
        context = {
            'code': code,
            'date': today_data.date.isoformat(),
            'today': today_data.to_dict(),
        }
        
        if yesterday_data:
            context['yesterday'] = yesterday_data.to_dict()
            
            # 计算相比昨日的变化
            if yesterday_data.volume and yesterday_data.volume > 0:
                context['volume_change_ratio'] = round(
                    today_data.volume / yesterday_data.volume, 2
                )
            
            if yesterday_data.close and yesterday_data.close > 0:
                context['price_change_ratio'] = round(
                    (today_data.close - yesterday_data.close) / yesterday_data.close * 100, 2
                )
            
            # 均线形态判断
            context['ma_status'] = self._analyze_ma_status(today_data)
        
        return context
    
    def _analyze_ma_status(self, data: StockDaily) -> str:
        """
        分析均线形态
        
        判断条件：
        - 多头排列：close > ma5 > ma10 > ma20
        - 空头排列：close < ma5 < ma10 < ma20
        - 震荡整理：其他情况
        """
        close = data.close or 0
        ma5 = data.ma5 or 0
        ma10 = data.ma10 or 0
        ma20 = data.ma20 or 0
        
        if close > ma5 > ma10 > ma20 > 0:
            return "多头排列 📈"
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "空头排列 📉"
        elif close > ma5 and ma5 > ma10:
            return "短期向好 🔼"
        elif close < ma5 and ma5 < ma10:
            return "短期走弱 🔽"
        else:
            return "震荡整理 ↔️"

    @staticmethod
    def _parse_published_date(value: Optional[str]) -> Optional[datetime]:
        """
        解析发布时间字符串（失败返回 None）
        """
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        text = str(value).strip()
        if not text:
            return None

        # 优先尝试 ISO 格式
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        return None

    @staticmethod
    def _safe_json_dumps(data: Any) -> str:
        """
        安全序列化为 JSON 字符串
        """
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except Exception:
            return json.dumps(str(data), ensure_ascii=False)

    @staticmethod
    def _build_raw_result(result: Any) -> Dict[str, Any]:
        """
        生成完整分析结果字典
        """
        data = result.to_dict() if hasattr(result, "to_dict") else {}
        data.update({
            'data_sources': getattr(result, 'data_sources', ''),
            'raw_response': getattr(result, 'raw_response', None),
        })
        return data

    @staticmethod
    def _parse_sniper_value(value: Any) -> Optional[float]:
        """
        解析狙击点位数值
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).replace(',', '').strip()
        if not text:
            return None

        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        try:
            return float(match.group())
        except ValueError:
            return None

    def _extract_sniper_points(self, result: Any) -> Dict[str, Optional[float]]:
        """
        抽取狙击点位数据
        """
        raw_points = {}
        if hasattr(result, "get_sniper_points"):
            raw_points = result.get_sniper_points() or {}

        return {
            "ideal_buy": self._parse_sniper_value(raw_points.get("ideal_buy")),
            "secondary_buy": self._parse_sniper_value(raw_points.get("secondary_buy")),
            "stop_loss": self._parse_sniper_value(raw_points.get("stop_loss")),
            "take_profit": self._parse_sniper_value(raw_points.get("take_profit")),
        }

    @staticmethod
    def _build_fallback_url_key(
        code: str,
        title: str,
        source: str,
        published_date: Optional[datetime]
    ) -> str:
        """
        生成无 URL 时的去重键（确保稳定且较短）
        """
        date_str = published_date.isoformat() if published_date else ""
        raw_key = f"{code}|{title}|{source}|{date_str}"
        digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()
        return f"no-url:{code}:{digest}"

    def save_stock_info(self, df: pd.DataFrame) -> int:
        """
        保存股票基础信息到数据库

        Args:
            df: 包含股票信息的 DataFrame

        Returns:
            新增/更新的记录数
        """
        if df is None or df.empty:
            return 0

        saved_count = 0

        with self.get_session() as session:
            try:
                for _, row in df.iterrows():
                    ts_code = row.get('ts_code')
                    if not ts_code:
                        continue

                    # 检查是否已存在
                    existing = session.execute(
                        select(StockInfo).where(StockInfo.ts_code == ts_code)
                    ).scalar_one_or_none()

                    if existing:
                        # 更新
                        for key, value in row.items():
                            if hasattr(existing, key):
                                setattr(existing, key, value)
                        existing.updated_at = datetime.now()
                    else:
                        # 插入
                        record = StockInfo(
                            ts_code=ts_code,
                            symbol=row.get('symbol'),
                            name=row.get('name'),
                            area=row.get('area'),
                            industry=row.get('industry'),
                            fullname=row.get('fullname'),
                            enname=row.get('enname'),
                            cnspell=row.get('cnspell'),
                            market=row.get('market'),
                            exchange=row.get('exchange'),
                            curr_type=row.get('curr_type'),
                            list_status=row.get('list_status'),
                            list_date=row.get('list_date'),
                            delist_date=row.get('delist_date'),
                            is_hs=row.get('is_hs'),
                            act_name=row.get('act_name'),
                            act_ent_type=row.get('act_ent_type'),
                        )
                        session.add(record)
                        saved_count += 1

                session.commit()
                logger.info(f"保存股票信息成功，新增 {saved_count} 条")

            except Exception as e:
                session.rollback()
                logger.error(f"保存股票信息失败: {e}")
                raise

        return saved_count

    def get_stock_info(self, symbol: Optional[str] = None, ts_code: Optional[str] = None, list_status: Optional[str] = None, market: Optional[Union[str, List[str]]] = None) -> List[StockInfo]:
        """
        查询股票信息

        Args:
            symbol: 股票代码（可选）
            ts_code: TS代码（可选）
            list_status: 上市状态（可选，L上市 D退市 P暂停上市）
            market: 市场类型（可选，支持单个字符串或字符串列表，如 '主板', '创业板'）

        Returns:
            StockInfo 对象列表
        """
        with self.get_session() as session:
            query = select(StockInfo)
            if symbol:
                query = query.where(StockInfo.symbol == symbol)
            if ts_code:
                query = query.where(StockInfo.ts_code == ts_code)
            if list_status:
                query = query.where(StockInfo.list_status == list_status)
            if market:
                if isinstance(market, list):
                    query = query.where(StockInfo.market.in_(market))
                else:
                    query = query.where(StockInfo.market == market)

            results = session.execute(query).scalars().all()
            return list(results)

    def save_strong_stock(self, data: Dict[str, Any]) -> bool:
        """
        保存强势股信息

        Args:
            data: 包含强势股信息的字典

        Returns:
            是否保存成功
        """
        code = data.get('code')
        if not code:
            return False

        with self.get_session() as session:
            try:
                # 检查是否已存在
                existing = session.execute(
                    select(StrongStockInfo).where(StrongStockInfo.code == code)
                ).scalar_one_or_none()

                if existing:
                    # 更新
                    existing.name = data.get('name')
                    existing.last_price = str(data.get('last_price'))
                    existing.ma5 = str(data.get('ma5'))
                    existing.ma10 = str(data.get('ma10'))
                    existing.ma20 = str(data.get('ma20'))
                    existing.industry = data.get('industry')
                    existing.ai_analysis = data.get('ai_analysis')
                    existing.strategy_match = data.get('strategy_match')
                    existing.market_value = data.get('market_value')
                    existing.pe_ratio = data.get('pe_ratio')
                    existing.date = date.today()
                    existing.updated_at = datetime.now()
                else:
                    # 插入
                    record = StrongStockInfo(
                        code=code,
                        name=data.get('name'),
                        last_price=str(data.get('last_price')),
                        ma5=str(data.get('ma5')),
                        ma10=str(data.get('ma10')),
                        ma20=str(data.get('ma20')),
                        industry=data.get('industry'),
                        ai_analysis=data.get('ai_analysis'),
                        strategy_match=data.get('strategy_match'),
                        market_value=data.get('market_value'),
                        pe_ratio=data.get('pe_ratio'),
                        date=date.today()
                    )
                    session.add(record)

                session.commit()
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"保存强势股 {code} 失败: {e}")
                return False


# 便捷函数
def get_db() -> DatabaseManager:
    """获取数据库管理器实例的快捷方式"""
    return DatabaseManager.get_instance()


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    db = get_db()
    
    print("=== 数据库测试 ===")
    print(f"数据库初始化成功")
    
    # 调试：打印所有 list_status 的值
    with db.get_session() as session:
        # 统计 list_status 的分布
        status_counts = session.execute(
            select(StockInfo.list_status, func.count(StockInfo.id))
            .group_by(StockInfo.list_status)
        ).all()

        print("\n=== 数据库中 list_status 分布 ===")
        if not status_counts:
            print("stock_info 表为空！")
        for status, count in status_counts:
            print(f"状态: '{status}', 数量: {count}")

        # 打印前5条数据
        print("\n=== 前5条数据示例 ===")
        sample_data = session.execute(select(StockInfo).limit(5)).scalars().all()
        for item in sample_data:
            print(f"TS代码: {item.ts_code}, 状态: '{item.list_status}'")

    # 测试查询股票信息
    print("\n=== 测试查询 ===")
    stock_infos = db.get_stock_info(list_status='L')
    print(f"查询 list_status='L' 结果数量: {len(stock_infos)}")

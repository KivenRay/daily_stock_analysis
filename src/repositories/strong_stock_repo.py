# -*- coding: utf-8 -*-
"""
===================================
强势股数据访问层
===================================

职责：
1. 封装强势股数据的数据库操作
2. 提供强势股查询接口
"""

import logging
from datetime import date
from typing import Optional, List, Dict, Any, Tuple

from sqlalchemy import select, desc, func

from src.storage import DatabaseManager, StrongStockInfo

logger = logging.getLogger(__name__)


class StrongStockRepository:
    """
    强势股数据访问层
    
    封装 StrongStockInfo 表的数据库操作
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        初始化数据访问层
        
        Args:
            db_manager: 数据库管理器（可选，默认使用单例）
        """
        self.db = db_manager or DatabaseManager.get_instance()
    
    def save(self, data: Dict[str, Any]) -> bool:
        """
        保存强势股信息
        
        Args:
            data: 包含强势股信息的字典
            
        Returns:
            是否保存成功
        """
        try:
            return self.db.save_strong_stock(data)
        except Exception as e:
            logger.error(f"保存强势股失败: {e}")
            return False
    
    def get_list(
        self,
        date_str: Optional[date] = None,
        page: int = 1,
        page_size: int = 20
    ) -> Tuple[List[StrongStockInfo], int]:
        """
        获取强势股列表（分页）
        
        Args:
            date_str: 筛选日期
            page: 页码
            page_size: 每页数量
            
        Returns:
            Tuple[List[StrongStockInfo], int]: (记录列表, 总数)
        """
        try:
            with self.db.get_session() as session:
                query = select(StrongStockInfo)
                
                if date_str:
                    query = query.where(StrongStockInfo.date == date_str)
                
                # 计算总数
                count_query = select(func.count()).select_from(query.subquery())
                total = session.execute(count_query).scalar() or 0
                
                # 分页查询
                query = query.order_by(desc(StrongStockInfo.date), StrongStockInfo.code)
                query = query.offset((page - 1) * page_size).limit(page_size)
                
                results = session.execute(query).scalars().all()
                
                return list(results), total
        except Exception as e:
            logger.error(f"获取强势股列表失败: {e}")
            return [], 0
    
    def get_by_code(self, code: str) -> Optional[StrongStockInfo]:
        """
        根据代码获取强势股信息
        
        Args:
            code: 股票代码
            
        Returns:
            StrongStockInfo 对象，不存在返回 None
        """
        try:
            with self.db.get_session() as session:
                result = session.execute(
                    select(StrongStockInfo).where(StrongStockInfo.code == code)
                ).scalar_one_or_none()
                return result
        except Exception as e:
            logger.error(f"获取强势股 {code} 失败: {e}")
            return None

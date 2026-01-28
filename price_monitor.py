# -*- coding: utf-8 -*-
"""
===================================
价格监控与消息推送模块
===================================

职责：
1. 从数据库获取 AI 推荐的股票列表
2. 实时监控股票价格（调用 AkshareFetcher）
3. 判断是否触发买入、止盈、止损条件
4. 发送推送消息并记录到数据库
5. 避免重复推送
"""

import logging
import time
from datetime import datetime
from typing import List, Optional

from storage import get_db, AIStockRecommendation, PushMessageRecord
from data_provider.akshare_fetcher import AkshareFetcher, RealtimeQuote
from notification import get_notification_service

logger = logging.getLogger(__name__)

class PriceMonitor:
    """
    价格监控器
    """
    
    # 消息类型常量
    MSG_TYPE_AI_ANALYSIS = 1
    MSG_TYPE_BUY = 2
    MSG_TYPE_TAKE_PROFIT = 3
    MSG_TYPE_STOP_LOSS = 4
    
    def __init__(self):
        self.db = get_db()
        self.fetcher = AkshareFetcher()
        self.notifier = get_notification_service()

    def _is_trading_time(self) -> bool:
        """
        判断当前是否为交易时间 (9:00 - 15:00)
        """
        now = datetime.now().time()
        start_time = datetime.strptime("09:00:00", "%H:%M:%S").time()
        end_time = datetime.strptime("15:00:00", "%H:%M:%S").time()
        return start_time <= now <= end_time

    def _check_price_condition(self, rec: AIStockRecommendation, quote: RealtimeQuote):
        """
        检查价格是否触发条件
        """
        current_price = quote.price
        stock_code = rec.stock_code
        stock_name = rec.stock_name
        
        # 1. 检查买入条件
        if rec.buy_price_min and rec.buy_price_max:
            if rec.buy_price_min <= current_price <= rec.buy_price_max:
                self._trigger_push(
                    rec, 
                    self.MSG_TYPE_BUY, 
                    current_price, 
                    f"[{rec.buy_price_min}, {rec.buy_price_max}]"
                )

        # 2. 检查止盈条件 (卖出)
        if rec.take_profit_price_min and rec.take_profit_price_max:
            # 只要进入止盈区间或者高于止盈区间，都算触发
            if current_price >= rec.take_profit_price_min:
                 self._trigger_push(
                    rec, 
                    self.MSG_TYPE_TAKE_PROFIT, 
                    current_price, 
                    f"[{rec.take_profit_price_min}, {rec.take_profit_price_max}]"
                )

        # 3. 检查止损条件
        if rec.stop_loss_price_min and rec.stop_loss_price_max:
            # 只要进入止损区间或者低于止损区间，都算触发
            if current_price <= rec.stop_loss_price_max:
                 self._trigger_push(
                    rec, 
                    self.MSG_TYPE_STOP_LOSS, 
                    current_price, 
                    f"[{rec.stop_loss_price_min}, {rec.stop_loss_price_max}]"
                )

    def _trigger_push(self, rec: AIStockRecommendation, msg_type: int, current_price: float, trade_range: str):
        """
        触发消息推送
        """
        # 检查当天是否已推送过同类型消息
        if self.db.has_pushed_today(rec.stock_code, msg_type):
            logger.debug(f"[{rec.stock_code}] {self._get_msg_type_name(msg_type)} 当天已推送，跳过")
            return

        # 构建消息内容
        type_name = self._get_msg_type_name(msg_type)
        
        # 使用 Markdown 格式构建消息，以便更好地展示
        content = (
            f"## 🔔 {type_name}提醒\n\n"
            f"**股票**：{rec.stock_name} ({rec.stock_code})\n"
            f"**当前价格**：{current_price}\n"
            f"**触发区间**：{trade_range}\n"
            f"**所属板块**：{rec.sector or '未知'}\n"
            f"**时间**：{datetime.now().strftime('%H:%M:%S')}\n\n"
            f"--- \n"
            f"*AI智能监控系统*"
        )
        
        # 保存记录
        record = PushMessageRecord(
            stock_code=rec.stock_code,
            stock_name=rec.stock_name,
            sector=rec.sector,
            message_type=msg_type,
            message_content=content,
            current_price=current_price,
            trade_range=trade_range
        )
        
        try:
            self.db.save_push_record(record)
            logger.info(f"🚀 触发推送: {rec.stock_name} {type_name}")
            
            # 调用 NotificationService 发送消息
            if self.notifier.is_available():
                self.notifier.send(content)
            else:
                logger.warning("通知服务不可用，仅保存记录")
                
        except Exception as e:
            logger.error(f"消息推送处理失败: {e}")

    def _get_msg_type_name(self, msg_type: int) -> str:
        if msg_type == self.MSG_TYPE_AI_ANALYSIS:
            return "AI分析"
        elif msg_type == self.MSG_TYPE_BUY:
            return "触发买入"
        elif msg_type == self.MSG_TYPE_TAKE_PROFIT:
            return "触发止盈"
        elif msg_type == self.MSG_TYPE_STOP_LOSS:
            return "触发止损"
        return "未知类型"

    def run_once(self):
        """
        执行一次完整的监控流程
        """
        # 检查交易时间
        if not self._is_trading_time():
            # logger.info("当前非交易时间 (9:00-15:00)，跳过监控") # 减少日志噪音
            return

        logger.info("开始执行价格监控...")
        
        # 1. 获取所有 AI 推荐记录
        recommendations = self.db.get_all_recommendations()
        if not recommendations:
            logger.info("没有找到 AI 推荐记录，监控结束")
            return

        logger.info(f"获取到 {len(recommendations)} 条推荐记录，开始获取实时行情...")

        for rec in recommendations:
            try:
                # 2. 获取实时行情
                quote = self.fetcher.get_realtime_quote(rec.stock_code)
                if not quote:
                    logger.warning(f"无法获取 {rec.stock_code} 的实时行情，跳过")
                    continue
                
                logger.info(f"监控中: {rec.stock_name}({rec.stock_code}) 现价: {quote.price}")

                # 3. 检查条件
                self._check_price_condition(rec, quote)
                
                # 避免请求过快
                time.sleep(1) 
                
            except Exception as e:
                logger.error(f"处理 {rec.stock_code} 时发生错误: {e}")

        logger.info("本次监控流程结束")

# 供 scheduler.py 调用的入口函数（虽然 scheduler.py 直接实例化 PriceMonitor，但保留此函数作为模块接口也是好的实践）
def run_price_monitor_task():
    """
    价格监控任务入口
    """
    monitor = PriceMonitor()
    monitor.run_once()

if __name__ == "__main__":
    # 测试模式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    )
    
    monitor = PriceMonitor()
    print("=== 测试模式：执行一次监控 ===")
    # 临时 mock _is_trading_time 为 True 以便测试
    original_check = monitor._is_trading_time
    monitor._is_trading_time = lambda: True
    monitor.run_once()
    monitor._is_trading_time = original_check

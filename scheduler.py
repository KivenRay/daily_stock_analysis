# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 定时任务调度器
===================================

职责：
1. 使用 schedule 库实现灵活的定时任务调度
2. 根据配置，动态添加不同类型的分析任务
3. 在独立的线程中运行调度器，避免阻塞主线程
"""

import logging
import time
import threading
from typing import List, Dict, Any, Callable
import importlib
import schedule
from config import get_config

logger = logging.getLogger(__name__)

# 任务类型到执行函数的映射（使用字符串，避免循环导入）
TASK_REGISTRY: Dict[str, str] = {
    "full_analysis": "main.run_full_analysis",
    "market_review": "main.run_market_review",
    "scan_market": "scanner_cn.scan_market",
    "price_monitor": "price_monitor.run_price_monitor_task", # 新增价格监控任务
}

def _run_task(task_name: str, task_config: Dict[str, Any]):
    """
    执行具体的定时任务
    
    Args:
        task_name: 任务名称
        task_config: 任务配置
    """
    logger.info(f"🚀 开始执行定时任务: {task_name}")
    
    task_path = TASK_REGISTRY.get(task_name)
    if not task_path:
        logger.error(f"未知的任务类型: {task_name}")
        return
        
    try:
        module_path, func_name = task_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        task_func = getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        logger.error(f"无法加载任务函数 '{task_path}': {e}")
        return

    try:
        # 在这里，我们为每个任务函数提供它需要的参数
        if task_name == "full_analysis":
            from argparse import Namespace
            args = Namespace(
                single_notify=task_config.get('single_notify', False), 
                workers=task_config.get('workers'), 
                dry_run=task_config.get('dry_run', False), 
                no_notify=task_config.get('no_notify', False),
                no_market_review=task_config.get('no_market_review', False)
            )
            task_func(get_config(), args, stock_codes=task_config.get('stock_codes'))
        elif task_name == "market_review":
            from notification import get_notification_service
            from analyzer import get_analyzer
            from search_service import get_search_service
            
            notifier = get_notification_service()
            analyzer = get_analyzer()
            search_service = get_search_service()
            task_func(notifier, analyzer, search_service)
        elif task_name == "scan_market":
            task_func()
        elif task_name == "price_monitor":
            task_func()
        else:
            task_func()
            
        logger.info(f"✅ 定时任务: {task_name} 执行完成")
        
    except Exception as e:
        logger.error(f"❌ 定时任务: {task_name} 执行失败: {e}", exc_info=True)

def setup_scheduler():
    """
    设置所有定时任务
    """
    config = get_config()
    tasks = config.scheduled_tasks
    
    if not tasks:
        logger.info("没有配置任何定时任务。")
        return
        
    logger.info(f"从 config.yml 加载了 {len(tasks)} 个定时任务，正在设置...")
    
    for task_idx, task in enumerate(tasks):
        task_type = task.get("type")
        task_time = task.get("time")
        day_of_week = task.get("day_of_week") # 新增：支持每周特定几天运行
        
        if not task_type or not task_time:
            logger.warning(f"跳过无效的定时任务配置 (索引 {task_idx}): {task}")
            continue
            
        if task_type not in TASK_REGISTRY:
            logger.warning(f"未知的任务类型 '{task_type}' (索引 {task_idx})，跳过。")
            continue
            
        try:
            from functools import partial
            job_func = partial(_run_task, task_name=task_type, task_config=task)
            
            # 根据 day_of_week 设置任务
            if day_of_week:
                days = day_of_week.split(',') # 允许逗号分隔，例如 "monday,tuesday"
                for day in days:
                    day = day.strip().lower()
                    if day == "monday":
                        schedule.every().monday.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                    elif day == "tuesday":
                        schedule.every().tuesday.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                    elif day == "wednesday":
                        schedule.every().wednesday.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                    elif day == "thursday":
                        schedule.every().thursday.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                    elif day == "friday":
                        schedule.every().friday.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                    elif day == "saturday":
                        schedule.every().saturday.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                    elif day == "sunday":
                        schedule.every().sunday.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                    else:
                        logger.warning(f"无效的 day_of_week '{day}' (任务 {task_idx})，跳过该天的设置。")
                logger.info(f"已设置任务 '{task_type}' (索引 {task_idx})，每周 {day_of_week} {task_time} 执行。")
            else:
                # 默认每日执行
                schedule.every().day.at(task_time).do(job_func).tag(f"{task_type}-{task_idx}")
                logger.info(f"已设置任务 '{task_type}' (索引 {task_idx})，每日 {task_time} 执行。")
            
        except Exception as e:
            logger.error(f"设置任务 '{task_type}' (索引 {task_idx}) 失败: {e}", exc_info=True)

    # === 新增：自动启动价格监控任务 ===
    # 价格监控不同于普通定时任务，它需要高频运行（如每分钟）
    # 因此我们不通过 config.yml 配置，而是直接在这里硬编码启动
    # try:
    #     from price_monitor import PriceMonitor
    #     monitor = PriceMonitor()
    #     # 每分钟执行一次监控
    #     schedule.every(3).minutes.do(monitor.run_once).tag("price_monitor_auto")
    #     logger.info("✅ 已自动设置价格监控任务，每1分钟执行一次。")
    # except Exception as e:
    #     logger.error(f"❌ 设置价格监控任务失败: {e}", exc_info=True)


def run_scheduler():
    """
    在独立的线程中运行定时任务调度器
    """
    setup_scheduler()
    
    def _scheduler_loop():
        logger.info("⏰ 定时任务调度器已启动...")
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    scheduler_thread.start()
    logger.info("调度器线程已在后台运行。")
    return scheduler_thread

if __name__ == "__main__":
    # 用于独立测试调度器
    logging.basicConfig(level=logging.INFO)
    
    # 为了测试，我们需要确保 main.py 中的函数可以被导入
    # 并且它们的依赖项也已正确设置
    
    print("正在设置并启动调度器进行测试...")
    run_scheduler()
    
    print("调度器正在后台运行，主线程将保持活跃。按 Ctrl+C 退出。")
    try:
        while True:
            time.sleep(60)
            print(f"[{datetime.now()}] 调度器仍在运行... 下一个任务在: {schedule.next_run}")
    except KeyboardInterrupt:
        print("\n正在退出...")

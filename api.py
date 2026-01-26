# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - FastAPI 接口
===================================

职责：
1. 提供 API 接口，用于获取数据和接收外部数据
2. 提供一个简单的 Web 页面，用于编辑自选股列表
3. 使用 FastAPI 框架，支持异步和 Pydantic 数据验证
"""
import json
import logging
import threading
import re
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Request, Header, HTTPException, Depends, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from storage import get_db, AIStockRecommendation
from scanner_cn import StrongStock
from config import get_config
from notification import get_notification_service
from scheduler import run_scheduler # 导入 run_scheduler
from sqlalchemy import select, desc, func

# 导入 main 模块中的分析函数，用于后台任务
# 注意：这里是局部导入，以避免循环依赖
# from main import run_full_analysis, run_market_review # 暂时不导入，因为 run_full_analysis 依赖 StockAnalysisPipeline

logger = logging.getLogger(__name__)

app = FastAPI(title="A股自选股智能分析系统 API", version="1.0")

# --- 启动事件 ---

@app.on_event("startup")
async def startup_event():
    """在 FastAPI 启动时，启动后台的定时任务调度器"""
    logger.info("FastAPI 应用启动，开始启动定时任务调度器...")
    run_scheduler()

# --- 依赖项：API Key 认证 ---

async def verify_api_key(x_api_key: str = Header(...)):
    """依赖项：验证 X-API-KEY"""
    config = get_config()
    if not config.webui_api_key:
        raise HTTPException(
            status_code=503, detail="API Key not configured on the server."
        )
    if x_api_key != config.webui_api_key:
        raise HTTPException(status_code=401, detail="Invalid API Key")

# --- Pydantic 模型：用于请求体验证 ---

class AIRecommendationIn(BaseModel):
    stock_code: str
    stock_name: str
    sector: Optional[str] = None
    ai_score: Optional[float] = None
    core_tags: Optional[str] = None
    analysis_info: Optional[str] = None
    buy_price_min: Optional[float] = None
    buy_price_max: Optional[float] = None
    take_profit_price_min: Optional[float] = None
    take_profit_price_max: Optional[float] = None
    stop_loss_price_min: Optional[float] = None
    stop_loss_price_max: Optional[float] = None
    is_push_msg: bool = False

class FeishuEvent(BaseModel):
    """飞书事件回调请求体"""
    challenge: Optional[str] = None
    token: Optional[str] = None
    type: Optional[str] = None
    event: Optional[Dict[str, Any]] = None

# --- 辅助函数 ---

# 用于跟踪正在进行分析的股票，防止重复触发
# Feishu 等平台可能会因为超时而重发事件，导致同一任务被多次触发
# 使用一个集合来存储正在分析的股票代码，作为简单的内存锁
# 注意：此锁仅在单 worker 模式下有效。若使用多 worker (如 gunicorn -w 4)，需改用 Redis 等分布式锁。
ANALYSIS_IN_PROGRESS = set()
ANALYSIS_LOCK = threading.Lock()

def _extract_stock_codes_from_text(text: str) -> List[str]:
    """从文本中提取股票代码"""
    # 匹配 6 位数字的股票代码
    # 考虑 A 股以 60, 00, 30 开头
    pattern = r'\b(?:60|00|30|68)\d{4}\b'
    codes = re.findall(pattern, text)
    return list(set(codes)) # 去重

def _perform_single_stock_analysis_task(stock_code: str):
    """
    在后台执行单只股票的分析任务 (这是一个阻塞型任务)
    
    Args:
        stock_code: 要分析的股票代码
    """
    # 加锁检查任务是否已在运行
    with ANALYSIS_LOCK:
        if stock_code in ANALYSIS_IN_PROGRESS:
            logger.warning(f"后台任务：股票 {stock_code} 的分析任务已在进行中，本次请求被跳过。")
            return
        # 如果不在运行，则加入集合，开始执行
        ANALYSIS_IN_PROGRESS.add(stock_code)

    try:
        # 局部导入，避免循环依赖
        from main import StockAnalysisPipeline
        from argparse import Namespace
        
        logger.info(f"后台任务：开始对股票 {stock_code} 进行单股分析...")
        
        config = get_config()
        # 模拟命令行参数，启用单股推送
        args = Namespace(
            single_notify=True, 
            workers=None, 
            dry_run=False, 
            no_notify=False,
            no_market_review=False
        )
        
        # 注意：StockAnalysisPipeline 是单例，这里的调用是安全的
        pipeline = StockAnalysisPipeline(config=config, max_workers=args.workers)
        
        # process_single_stock 是一个阻塞函数，包含网络IO和CPU密集型任务
        # BackgroundTasks 会在单独的线程中运行它，不会阻塞 FastAPI 的事件循环
        result = pipeline.process_single_stock(stock_code, single_stock_notify=True)
        if result and result.success:
            logger.info(f"后台任务：股票 {stock_code} 单股分析完成并已推送。")
        else:
            logger.warning(f"后台任务：股票 {stock_code} 单股分析失败或未生成结果。")
    except Exception as e:
        logger.error(f"后台任务：股票 {stock_code} 单股分析异常: {e}", exc_info=True)
    finally:
        # 任务完成或失败后，从集合中移除，以便下次可以执行
        with ANALYSIS_LOCK:
            ANALYSIS_IN_PROGRESS.remove(stock_code)


# --- API 接口 ---

@app.get("/api/strong_stocks", dependencies=[Depends(verify_api_key)])
async def get_strong_stocks():
    """获取最新一天的强势股票数据"""
    db = get_db()
    with db.get_session() as session:
        try:
            latest_scan_time_query = select(func.date(StrongStock.scan_time)).order_by(desc(StrongStock.scan_time)).limit(1)
            latest_date = session.execute(latest_scan_time_query).scalar_one_or_none()

            if not latest_date:
                return JSONResponse(content={"message": "暂无强势股票数据"}, status_code=200)

            strong_stocks_query = select(StrongStock).filter(func.date(StrongStock.scan_time) == latest_date)
            strong_stocks = session.execute(strong_stocks_query).scalars().all()
            
            return [s.to_dict() for s in strong_stocks]
        except Exception as e:
            logger.error(f"获取强势股票数据失败: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"服务器内部错误: {str(e)}")

@app.post("/api/ai_recommendations", dependencies=[Depends(verify_api_key)])
async def create_ai_recommendations(recommendations: List[AIRecommendationIn]):
    """接收并保存 AI 股票推荐数据列表"""
    db = get_db()
    notifier = get_notification_service()
    
    success_count = 0
    failed_items = []

    for item in recommendations:
        try:
            recommendation = AIStockRecommendation(**item.dict())
            
            saved_recommendation = db.save_ai_recommendation(recommendation)
            
            if not saved_recommendation.is_push_msg:
                notification_success = notifier.send_ai_recommendation_notification(saved_recommendation)
                if notification_success:
                    with db.get_session() as session:
                        saved_recommendation.is_push_msg = True
                        session.add(saved_recommendation)
                        session.commit()
            
            success_count += 1
        except Exception as e:
            logger.error(f"处理AI推荐数据失败: {item.dict()}", exc_info=True)
            failed_items.append({"item": item.dict(), "error": str(e)})

    if not failed_items:
        return {"message": f"成功处理 {success_count} 条AI推荐数据。"}
    else:
        return JSONResponse(
            status_code=207,
            content={
                "message": f"处理完成，成功 {success_count} 条，失败 {len(failed_items)} 条。",
                "failed_items": failed_items,
            },
        )

@app.post("/feishu/event")
async def feishu_event_callback(event_data: FeishuEvent, background_tasks: BackgroundTasks):
    """
    处理飞书事件回调
    
    包括 URL 验证和消息接收
    """
    logger.info(f"收到飞书事件回调: {event_data.dict()}")

    # URL 验证
    if event_data.type == "url_verification" and event_data.challenge:
        logger.info(f"飞书 URL 验证请求，challenge: {event_data.challenge}")
        return JSONResponse(content={"challenge": event_data.challenge})

    # 消息接收事件
    if event_data.event:
        message = event_data.event.get('message', {})
        message_type = message.get('message_type')
        content_str = message.get('content')
        
        if message_type == 'text' and content_str:
            try:
                # 飞书文本消息的 content 是 JSON 字符串
                message_content = json.loads(content_str)
                text = message_content.get('text', '')
                logger.info(f"收到飞书文本消息: {text}")

                # 指令分发
                if "分析" in text or "诊断" in text:
                    stock_codes = _extract_stock_codes_from_text(text)
                    if stock_codes:
                        logger.info(f"从飞书消息中提取到股票代码: {stock_codes}")
                        for code in stock_codes:
                            background_tasks.add_task(_perform_single_stock_analysis_task, code)
                        return JSONResponse(content={"message": "已接收股票代码，正在后台分析。"})
                    else:
                        return JSONResponse(content={"message": "请在消息中提供股票代码。"})
                else:
                    # 默认行为：如果消息中只包含股票代码，也执行分析
                    stock_codes = _extract_stock_codes_from_text(text)
                    if stock_codes:
                        logger.info(f"从飞书消息中提取到股票代码: {stock_codes}")
                        for code in stock_codes:
                            background_tasks.add_task(_perform_single_stock_analysis_task, code)
                        return JSONResponse(content={"message": "已接收股票代码，正在后台分析。"})
                    else:
                        logger.info("飞书消息中未包含可识别的指令或股票代码。")
                        return JSONResponse(content={"message": "未识别到有效指令或股票代码。"})
            except json.JSONDecodeError:
                logger.warning(f"飞书消息内容不是有效的 JSON: {content_str}")
                return JSONResponse(content={"message": "消息内容解析失败。"})
        else:
            logger.info(f"收到非文本飞书消息或空消息，类型: {message_type}")
            return JSONResponse(content={"message": "已接收非文本消息。"})

    return JSONResponse(content={"message": "事件已处理。"})


# --- Web 页面 ---

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """显示编辑自选股列表的页面"""
    # 这部分逻辑与 webui.py 中的 _page 和 _extract_stock_list 类似
    # 为了简化，我们直接在这里实现
    from webui import _read_env_text, _extract_stock_list, _page
    env_text = _read_env_text(".env")
    current_value = _extract_stock_list(env_text)
    return HTMLResponse(content=_page(current_value))

@app.post("/update", response_class=HTMLResponse)
async def update_stock_list(stock_list: str = Form(...)):
    """更新自选股列表"""
    from webui import _read_env_text, _set_stock_list, _write_env_text, _page, _normalize_stock_list
    env_text = _read_env_text(".env")
    updated_text = _set_stock_list(env_text, stock_list)
    _write_env_text(".env", updated_text)
    
    normalized_value = _normalize_stock_list(stock_list)
    return HTMLResponse(content=_page(normalized_value, message="已保存"))

def run_api_server():
    """启动 FastAPI 服务"""
    import uvicorn
    config = get_config()
    uvicorn.run(
        app,
        host=config.webui_host,
        port=config.webui_port,
        log_level="info"
    )

if __name__ == "__main__":
    # 用于独立测试 API 服务
    logging.basicConfig(level=logging.INFO)
    run_api_server()

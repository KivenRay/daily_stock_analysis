# -*- coding: utf-8 -*-
import logging
from typing import List, Optional
from datetime import date

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks

from src.repositories.strong_stock_repo import StrongStockRepository
from api.v1.schemas.strongstocks import StrongStock, StrongStockResponse

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get(
    "/",
    response_model=StrongStockResponse,
    summary="获取强势股列表",
    description="获取筛选出的强势股列表，支持分页和日期筛选"
)
def get_strong_stocks(
    date_str: Optional[date] = Query(None, alias="date", description="筛选日期"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量")
) -> StrongStockResponse:
    """
    获取强势股列表
    """
    try:
        repo = StrongStockRepository()
        results, total = repo.get_list(date_str=date_str, page=page, page_size=page_size)
        
        items = [StrongStock.from_orm(item) for item in results]
        
        return StrongStockResponse(total=total, items=items)
            
    except Exception as e:
        logger.error(f"获取强势股列表失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"获取强势股列表失败: {str(e)}")


@router.post(
    "/scan",
    summary="触发强势股扫描",
    description="异步触发全市场强势股扫描任务（耗时操作，后台运行）"
)
async def trigger_scan(background_tasks: BackgroundTasks):
    """
    触发扫描任务
    """
    from src.search_stock import StockAnalyzer
    
    def run_scan_task():
        logger.info("收到 API 触发扫描请求，开始执行...")
        try:
            analyzer = StockAnalyzer()
            results = analyzer.scan_strong_stocks()
            logger.info(f"API 触发扫描完成，找到 {len(results)} 只强势股")
        except Exception as e:
            logger.error(f"API 触发扫描失败: {e}", exc_info=True)

    # 添加到后台任务队列
    background_tasks.add_task(run_scan_task)
    
    return {
        "message": "强势股扫描任务已在后台启动", 
        "status": "processing",
        "note": "扫描可能需要几分钟时间，请稍后刷新列表查看结果"
    }

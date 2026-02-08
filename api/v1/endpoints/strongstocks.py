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

# -*- coding: utf-8 -*-
from typing import Optional, Union
from pydantic import BaseModel
from datetime import date, datetime

class StrongStock(BaseModel):
    code: str
    name: Optional[str] = None
    last_price: Optional[str] = None
    ma5: Optional[str] = None
    ma10: Optional[str] = None
    ma20: Optional[str] = None
    industry: Optional[str] = None
    ai_analysis: Optional[str] = None
    strategy_match: Optional[str] = None
    # 允许 date 或 datetime 类型，或者 None
    date: Optional[Union[date, datetime]] = None

    class Config:
        from_attributes = True

class StrongStockResponse(BaseModel):
    total: int
    items: list[StrongStock]

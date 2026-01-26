# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 配置管理模块
===================================

职责：
1. 使用单例模式管理全局配置
2. 从 config.yml 文件加载配置
3. 提供类型安全的配置访问接口
"""

import os
import yaml
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field

@dataclass
class Config:
    """
    系统配置类 - 单例模式
    
    设计说明：
    - 使用 dataclass 简化配置属性定义
    - 所有配置项从 config.yml 读取，支持默认值
    - 类方法 get_instance() 实现单例访问
    """
    
    # === 自选股配置 ===
    stock_list: List[str] = field(default_factory=list)

    # === 飞书云文档配置 ===
    feishu_app_id: Optional[str] = None
    feishu_app_secret: Optional[str] = None
    feishu_folder_token: Optional[str] = None

    # === 数据源 API Token ===
    tushare_token: Optional[str] = None
    
    # === AI 分析配置 ===
    gemini_api_key: Optional[str] = None
    gemini_model: str = "gemini-1.5-flash"
    gemini_model_fallback: str = "gemini-pro"
    gemini_request_delay: float = 2.0
    gemini_max_retries: int = 5
    gemini_retry_delay: float = 5.0

    # OpenAI 兼容 API（备选）
    openai_api_key: Optional[str] = None
    openai_base_url: Optional[str] = None
    openai_model: str = "gpt-4o-mini"
    
    # === 搜索引擎配置 ===
    bocha_api_keys: List[str] = field(default_factory=list)
    tavily_api_keys: List[str] = field(default_factory=list)
    serpapi_keys: List[str] = field(default_factory=list)
    
    # === 通知配置 ===
    wechat_webhook_url: Optional[str] = None
    feishu_webhook_url: Optional[str] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    email_sender: Optional[str] = None
    email_password: Optional[str] = None
    email_receivers: List[str] = field(default_factory=list)
    pushover_user_key: Optional[str] = None
    pushover_api_token: Optional[str] = None
    custom_webhook_urls: List[str] = field(default_factory=list)
    custom_webhook_bearer_token: Optional[str] = None
    single_stock_notify: bool = False
    feishu_max_bytes: int = 20000
    wechat_max_bytes: int = 4000
    
    # === 数据库配置 ===
    database_path: str = "./data/stock_analysis.db"
    
    # === 日志配置 ===
    log_dir: str = "./logs"
    log_level: str = "INFO"
    
    # === 系统配置 ===
    max_workers: int = 3
    debug: bool = False
    
    # === 定时任务配置 ===
    scheduled_tasks: List[Dict[str, Any]] = field(default_factory=list) # 重新添加 scheduled_tasks 字段
    market_review_enabled: bool = True
    
    # === 流控配置 ===
    akshare_sleep_min: float = 2.0
    akshare_sleep_max: float = 5.0
    tushare_rate_limit_per_minute: int = 80
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    
    # === API 服务配置 ===
    webui_host: str = "127.0.0.1"
    webui_port: int = 8000
    webui_api_key: Optional[str] = None
    
    # 单例实例存储
    _instance: Optional['Config'] = None
    
    @classmethod
    def get_instance(cls) -> 'Config':
        """获取配置单例实例"""
        if cls._instance is None:
            cls._instance = cls._load_from_yaml()
        return cls._instance
    
    @classmethod
    def _load_from_yaml(cls) -> 'Config':
        """从 config.yml 文件加载配置"""
        config_path = Path(__file__).parent / 'config.yml'
        if not config_path.exists():
            raise FileNotFoundError("未找到 config.yml 配置文件。")

        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)

        return cls(
            stock_list=config_data.get('stock_list', []),
            feishu_app_id=config_data.get('feishu_app_id'),
            feishu_app_secret=config_data.get('feishu_app_secret'),
            feishu_folder_token=config_data.get('feishu_folder_token'),
            tushare_token=config_data.get('tushare_token'),
            gemini_api_key=config_data.get('gemini_api_key'),
            gemini_model=config_data.get('gemini_model', 'gemini-1.5-flash'),
            gemini_model_fallback=config_data.get('gemini_model_fallback', 'gemini-pro'),
            gemini_request_delay=float(config_data.get('gemini_request_delay', 2.0)),
            gemini_max_retries=int(config_data.get('gemini_max_retries', 5)),
            gemini_retry_delay=float(config_data.get('gemini_retry_delay', 5.0)),
            openai_api_key=config_data.get('openai_api_key'),
            openai_base_url=config_data.get('openai_base_url'),
            openai_model=config_data.get('openai_model', 'gpt-4o-mini'),
            bocha_api_keys=config_data.get('bocha_api_keys', []),
            tavily_api_keys=config_data.get('tavily_api_keys', []),
            serpapi_keys=config_data.get('serpapi_keys', []),
            wechat_webhook_url=config_data.get('wechat_webhook_url'),
            feishu_webhook_url=config_data.get('feishu_webhook_url'),
            telegram_bot_token=config_data.get('telegram_bot_token'),
            telegram_chat_id=config_data.get('telegram_chat_id'),
            email_sender=config_data.get('email_sender'),
            email_password=config_data.get('email_password'),
            email_receivers=config_data.get('email_receivers', []),
            pushover_user_key=config_data.get('pushover_user_key'),
            pushover_api_token=config_data.get('pushover_api_token'),
            custom_webhook_urls=config_data.get('custom_webhook_urls', []),
            custom_webhook_bearer_token=config_data.get('custom_webhook_bearer_token'),
            single_stock_notify=bool(config_data.get('single_stock_notify', False)),
            feishu_max_bytes=int(config_data.get('feishu_max_bytes', 20000)),
            wechat_max_bytes=int(config_data.get('wechat_max_bytes', 4000)),
            database_path=config_data.get('database_path', './data/stock_analysis.db'),
            log_dir=config_data.get('log_dir', './logs'),
            log_level=config_data.get('log_level', 'INFO'),
            max_workers=int(config_data.get('max_workers', 3)),
            debug=bool(config_data.get('debug', False)),
            scheduled_tasks=config_data.get('scheduled_tasks', []), # 从 config_data 读取 scheduled_tasks
            market_review_enabled=bool(config_data.get('market_review_enabled', True)),
            webui_host=config_data.get('webui_host', '127.0.0.1'),
            webui_port=int(config_data.get('webui_port', 8000)),
            webui_api_key=config_data.get('webui_api_key'),
        )
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（主要用于测试）"""
        cls._instance = None

    def refresh_stock_list(self) -> None:
        """从 config.yml 热读取 stock_list"""
        config_path = Path(__file__).parent / 'config.yml'
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
                self.stock_list = config_data.get('stock_list', [])
    
    def validate(self) -> List[str]:
        """验证配置完整性"""
        warnings = []
        if not self.stock_list:
            warnings.append("警告：未配置自选股列表 (stock_list)")
        if not self.gemini_api_key and not self.openai_api_key:
            warnings.append("警告：未配置 Gemini 或 OpenAI API Key，AI 分析功能将不可用")
        return warnings
    
    def get_db_url(self) -> str:
        """获取 SQLAlchemy 数据库连接 URL"""
        db_path = Path(self.database_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{db_path.absolute()}"

def get_config() -> Config:
    """获取全局配置实例的快捷方式"""
    return Config.get_instance()

if __name__ == "__main__":
    config = get_config()
    print("=== 配置加载测试 ===")
    print(f"自选股列表: {config.stock_list}")
    print(f"数据库路径: {config.database_path}")
    print(f"最大并发数: {config.max_workers}")
    print(f"调试模式: {config.debug}")
    
    warnings = config.validate()
    if warnings:
        print("\n配置验证结果:")
        for w in warnings:
            print(f"  - {w}")

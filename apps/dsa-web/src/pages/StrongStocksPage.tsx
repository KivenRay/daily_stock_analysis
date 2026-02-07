import React, { useState, useEffect } from 'react';
import { Card } from '../components/common';

interface StrongStock {
    market_value: string;
    pe_ratio: string;
    code: string;
    name: string;
    last_price: string;
    ma5: string;
    ma10: string;
    ma20: string;
    industry: string;
    ai_analysis: string;
    strategy_match: string;
    date: string;
}

interface StrongStockResponse {
    total: number;
    items: StrongStock[];
}

const StrongStocksPage: React.FC = () => {
    const [stocks, setStocks] = useState<StrongStock[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [page, setPage] = useState(1);
    const [total, setTotal] = useState(0);
    const pageSize = 20;

    const fetchStocks = async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await fetch(`/api/v1/strongStocks/?page=${page}&page_size=${pageSize}`);
            if (!response.ok) {
                throw new Error('Failed to fetch strong stocks');
            }
            const data: StrongStockResponse = await response.json();
            setStocks(data.items);
            setTotal(data.total);
        } catch (err) {
            setError(err instanceof Error ? err.message : 'An unknown error occurred');
        } finally {
            setLoading(false);
        }
    };

    const handleScan = async () => {
        try {
            const response = await fetch('/api/v1/strongStocks/scan', {
                method: 'POST',
            });
            if (!response.ok) {
                throw new Error('Failed to trigger scan');
            }
            alert('扫描任务已启动，请稍后刷新页面查看结果');
        } catch (err) {
            alert('触发扫描失败: ' + (err instanceof Error ? err.message : 'Unknown error'));
        }
    };

    useEffect(() => {
        fetchStocks();
    }, [page]);

    return (
        <div className="min-h-screen flex flex-col p-6">
            <div className="flex justify-between items-center mb-6">
                <h1 className="text-2xl font-bold text-white">强势股列表</h1>
                <button
                    onClick={handleScan}
                    className="btn-primary flex items-center gap-1.5 whitespace-nowrap"
                >
                    触发扫描
                </button>
            </div>

            {error && (
                <div className="p-4 bg-danger/10 text-danger rounded-md mb-6 border border-danger/20">
                    {error}
                </div>
            )}

            <Card className="overflow-hidden border border-white/5 bg-surface">
                <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-white/5">
                        <thead className="bg-white/5">
                            <tr>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">代码</th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">名称</th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">价格</th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">市值</th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">市盈率</th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">行业</th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">策略匹配</th>
                                <th className="px-6 py-3 text-left text-xs font-medium text-secondary uppercase tracking-wider">日期</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-white/5">
                            {loading ? (
                                <tr>
                                    <td colSpan={6} className="px-6 py-8 text-center text-secondary">
                                        <div className="flex justify-center items-center gap-2">
                                            <div className="w-4 h-4 border-2 border-cyan/20 border-t-cyan rounded-full animate-spin" />
                                            加载中...
                                        </div>
                                    </td>
                                </tr>
                            ) : stocks.length === 0 ? (
                                <tr>
                                    <td colSpan={6} className="px-6 py-8 text-center text-muted">暂无数据</td>
                                </tr>
                            ) : (
                                stocks.map((stock) => (
                                    <tr key={stock.code} className="hover:bg-white/5 transition-colors">
                                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-cyan">{stock.code}</td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-white">{stock.name}</td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-white">{stock.last_price}</td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-white">{stock.market_value}</td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-white">{stock.pe_ratio}</td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary">{stock.industry}</td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-secondary">{stock.strategy_match || stock.ai_analysis}</td>
                                        <td className="px-6 py-4 whitespace-nowrap text-sm text-muted">{stock.date}</td>
                                    </tr>
                                ))
                            )}
                        </tbody>
                    </table>
                </div>
                
                {/* 分页控件 */}
                <div className="flex justify-between items-center px-6 py-4 border-t border-white/5 bg-white/5">
                    <div className="text-sm text-secondary">
                        共 {total} 条记录
                    </div>
                    <div className="flex space-x-2">
                        <button
                            onClick={() => setPage(p => Math.max(1, p - 1))}
                            disabled={page === 1}
                            className="px-3 py-1 border border-white/10 rounded-md text-sm text-secondary hover:bg-white/5 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                            上一页
                        </button>
                        <span className="px-3 py-1 text-sm text-white flex items-center">第 {page} 页</span>
                        <button
                            onClick={() => setPage(p => p + 1)}
                            disabled={stocks.length < pageSize}
                            className="px-3 py-1 border border-white/10 rounded-md text-sm text-secondary hover:bg-white/5 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                            下一页
                        </button>
                    </div>
                </div>
            </Card>
        </div>
    );
};

export default StrongStocksPage;

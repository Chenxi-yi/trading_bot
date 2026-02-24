# trading_bot

港股/美股日频选股（免费数据源版）。

## 目标
每天 **08:30 (Asia/Shanghai)** 生成策略日报：
- 港股 Top5
- 美股 Top5
- 先做流动性预筛（按 ADV20 前100）
- 按你的 8 条规则打分（100 分制）

## 策略规则
1. 主触发：短周期价格通道超过长周期价格通道  
2. 辅助：突破当天/次日放量  
3. 辅助：收盘价连续两天站在通道上沿  
4. 辅助：MACD 金叉或 MACD 在零轴上方  
5. 过滤：ATR 太低不做  
6. 过滤：大盘同向过滤  
7. 退出：跌回短通道内  
8. 退出：MACD 死叉且缩量  

## 数据源
- 主：`yfinance`（免费，覆盖美股+港股）
- 股票池：
  - 美股：NasdaqTrader 全量符号（去测试/ETF后）
  - 港股：HKEX `ListOfSecurities.xlsx` 提取代码

## 快速开始
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python src/run_daily.py
```

输出：
- `reports/daily/YYYY-MM-DD.md`
- `reports/daily/YYYY-MM-DD_us.csv`
- `reports/daily/YYYY-MM-DD_hk.csv`

## 参数验证（稳健性）
```bash
python src/validate_params.py
```
输出：`reports/backtest/param_scan.csv`

> 验证流程：in-sample → out-of-sample → walk-forward（下一步可继续扩展）。

## 自动化（GitHub Actions）
已提供 `.github/workflows/daily_report.yml`，默认 UTC `00:30`（即北京时间 08:30）运行并上传日报 artifact。

如果要推送到 Discord 频道：
1. 创建 Discord Incoming Webhook
2. 在仓库 Secrets 添加 `DISCORD_WEBHOOK_URL`
3. 工作流会自动把日报发送到 webhook

## 风险声明
仅用于研究与观察，不构成投资建议。

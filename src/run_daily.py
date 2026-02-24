from __future__ import annotations

from datetime import datetime
from pathlib import Path
import yaml
import pandas as pd
import yfinance as yf

from data_sources import get_us_universe, get_hk_universe, ensure_dirs
from strategy import evaluate_symbol

BASE = Path(__file__).resolve().parents[1]


def load_cfg():
    with open(BASE / "config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def market_trend_ok(benchmark: str, period="18mo") -> bool:
    df = yf.download(benchmark, period=period, interval="1d", auto_adjust=False, progress=False, threads=False)
    if df is None or df.empty or len(df) < 70:
        return False
    close = df["Close"]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    return bool((close.iloc[-1] > ma20) and (ma20 > ma60))


def fetch_ohlcv(symbol: str, period="18mo") -> pd.DataFrame:
    df = yf.download(symbol, period=period, interval="1d", auto_adjust=False, progress=False, threads=False)
    if df is None or df.empty:
        return pd.DataFrame()
    return df[["Open", "High", "Low", "Close", "Volume"]].dropna()


def run_market(market: str, symbols: list[str], cfg: dict):
    mcfg = cfg["markets"][market]
    scfg = cfg["strategy"]
    weights = cfg["scoring"]

    mkt_ok = market_trend_ok(mcfg["benchmark"])
    rows = []

    for sym in symbols:
        try:
            df = fetch_ohlcv(sym)
            res = evaluate_symbol(df, mkt_ok=mkt_ok, cfg=scfg, weights=weights)
            if res is None:
                continue
            rows.append({"symbol": sym, **res})
        except Exception:
            continue

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out = out[(out["eligible"]) & (out["rule_1_breakout"]) & (out["rule_2_volume"]) & (out["rule_3_hold_above"]) & (out["rule_4_macd"])]
    out = out.sort_values(["score", "symbol"], ascending=[False, True]).head(mcfg["top_n"]).reset_index(drop=True)
    return out


def to_line(r: pd.Series):
    badges = []
    for k in [
        "rule_1_breakout",
        "rule_2_volume",
        "rule_3_hold_above",
        "rule_4_macd",
        "rule_5_atr_filter",
        "rule_6_trend_filter",
        "rule_7_not_fall_back",
        "rule_8_not_bear_cross_shrink",
    ]:
        badges.append(f"{k.split('_')[1]}={'✅' if bool(r[k]) else '❌'}")
    return f"- {r['symbol']} | 分数: {int(r['score'])}/100 | " + " ".join(badges)


def main():
    cfg = load_cfg()
    ensure_dirs(BASE)

    us_symbols = get_us_universe(cfg["markets"]["us"]["max_symbols"])
    hk_symbols = get_hk_universe(cfg["markets"]["hk"]["max_symbols"])

    us = run_market("us", us_symbols, cfg)
    hk = run_market("hk", hk_symbols, cfg)

    now = datetime.now().strftime("%Y-%m-%d")
    report_md = [f"# 【策略日报】{now} {cfg['report_time']}", "", "## 港股 Top5"]
    if hk.empty:
        report_md.append("- 今日无满足全部主+辅条件的港股标的")
    else:
        report_md += [to_line(r) for _, r in hk.iterrows()]

    report_md += ["", "## 美股 Top5"]
    if us.empty:
        report_md.append("- 今日无满足全部主+辅条件的美股标的")
    else:
        report_md += [to_line(r) for _, r in us.iterrows()]

    report_md += [
        "",
        "## 备注",
        "- 数据源: yfinance（免费）",
        "- 美股使用上一个交易日收盘数据；港股使用上一个交易日收盘数据",
        "- 打分基于你的8条规则，满分100",
    ]

    report_file = BASE / "reports" / "daily" / f"{now}.md"
    report_file.write_text("\n".join(report_md), encoding="utf-8")

    if not us.empty:
        us.to_csv(BASE / "reports" / "daily" / f"{now}_us.csv", index=False)
    if not hk.empty:
        hk.to_csv(BASE / "reports" / "daily" / f"{now}_hk.csv", index=False)

    print(report_file)


if __name__ == "__main__":
    main()

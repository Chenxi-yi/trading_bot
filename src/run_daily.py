from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, List
import math
import yaml
import pandas as pd
import yfinance as yf

from data_sources import get_us_universe, get_hk_universe, ensure_dirs
from strategy import evaluate_symbol

BASE = Path(__file__).resolve().parents[1]


def load_cfg():
    with open(BASE / "config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _to_series(x):
    if isinstance(x, pd.DataFrame):
        return x.iloc[:, 0]
    return x


def market_trend_ok(benchmark: str, period="18mo") -> bool:
    df = yf.download(benchmark, period=period, interval="1d", auto_adjust=False, progress=False, threads=False)
    if df is None or df.empty or len(df) < 70:
        return False
    close = _to_series(df["Close"])
    ma20 = _to_series(close.rolling(20).mean()).iloc[-1]
    ma60 = _to_series(close.rolling(60).mean()).iloc[-1]
    last = _to_series(close).iloc[-1]
    return bool((float(last) > float(ma20)) and (float(ma20) > float(ma60)))


def fetch_ohlcv(symbol: str, period="18mo") -> pd.DataFrame:
    df = yf.download(symbol, period=period, interval="1d", auto_adjust=False, progress=False, threads=False)
    if df is None or df.empty:
        return pd.DataFrame()
    out = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = out.columns.get_level_values(0)
    return out.dropna()


def fetch_batch(symbols: List[str], period: str) -> Dict[str, pd.DataFrame]:
    if not symbols:
        return {}

    raw = yf.download(
        symbols,
        period=period,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=True,
        group_by="ticker",
    )
    out: Dict[str, pd.DataFrame] = {}
    if raw is None or raw.empty:
        return out

    # single ticker may return simple columns
    if not isinstance(raw.columns, pd.MultiIndex):
        cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in raw.columns]
        if len(cols) == 5:
            out[symbols[0]] = raw[cols].dropna()
        return out

    for sym in symbols:
        try:
            if sym not in raw.columns.get_level_values(0):
                continue
            part = raw[sym]
            cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in part.columns]
            if len(cols) < 5:
                continue
            out[sym] = part[cols].dropna()
        except Exception:
            continue
    return out


def select_liquid_symbols(symbols: List[str], top_n: int = 100, chunk_size: int = 150) -> List[str]:
    rows = []
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i : i + chunk_size]
        data_map = fetch_batch(chunk, period="3mo")
        for sym, df in data_map.items():
            if len(df) < 25:
                continue
            dv = (df["Close"] * df["Volume"]).tail(20).mean()
            if pd.isna(dv) or not math.isfinite(float(dv)):
                continue
            rows.append((sym, float(dv)))

    if not rows:
        return []
    ranked = sorted(rows, key=lambda x: x[1], reverse=True)
    return [s for s, _ in ranked[:top_n]]


def run_market(market: str, symbols: list[str], cfg: dict):
    mcfg = cfg["markets"][market]
    scfg = cfg["strategy"]
    weights = cfg["scoring"]

    mkt_ok = market_trend_ok(mcfg["benchmark"])
    liquid_top_n = int(mcfg.get("liquidity_top_n", 100))
    liquid_syms = select_liquid_symbols(symbols, top_n=liquid_top_n)

    rows = []
    fetch_ok = 0
    insufficient_history = 0

    for sym in liquid_syms:
        try:
            df = fetch_ohlcv(sym)
            if not df.empty:
                fetch_ok += 1
            res = evaluate_symbol(df, mkt_ok=mkt_ok, cfg=scfg, weights=weights)
            if res is None:
                insufficient_history += 1
                continue
            rows.append({"symbol": sym, **res})
        except Exception:
            continue

    if not rows:
        diagnostics = {
            "mkt_ok": mkt_ok,
            "liquid_selected": len(liquid_syms),
            "fetched_ok": fetch_ok,
            "insufficient_history": insufficient_history,
            "evaluated": 0,
            "pass_r1": 0,
            "pass_r2": 0,
            "pass_r1r2": 0,
            "pass_eligible": 0,
            "a_pool": 0,
            "b_pool": 0,
        }
        return pd.DataFrame(), pd.DataFrame(), liquid_top_n, len(liquid_syms), diagnostics

    out = pd.DataFrame(rows)

    # A档：保持严格版（原规则不变）
    a_pool = out[
        (out["eligible"])
        & (out["rule_1_breakout"])
        & (out["rule_2_volume"])
        & (out["rule_3_hold_above"])
        & (out["rule_4_macd"])
    ]
    a_tier = a_pool.sort_values(["score", "symbol"], ascending=[False, True]).head(mcfg["top_n"]).reset_index(drop=True)

    # B档：必须满足 主触发 + 量能确认，其余按得分排序
    b_pool_df = out[(out["rule_1_breakout"]) & (out["rule_2_volume"])]
    b_tier = b_pool_df.sort_values(["score", "symbol"], ascending=[False, True]).head(mcfg["top_n"]).reset_index(drop=True)

    diagnostics = {
        "mkt_ok": mkt_ok,
        "liquid_selected": len(liquid_syms),
        "fetched_ok": fetch_ok,
        "insufficient_history": insufficient_history,
        "evaluated": int(len(out)),
        "pass_r1": int(out["rule_1_breakout"].sum()),
        "pass_r2": int(out["rule_2_volume"].sum()),
        "pass_r1r2": int(((out["rule_1_breakout"]) & (out["rule_2_volume"])).sum()),
        "pass_eligible": int(out["eligible"].sum()),
        "a_pool": int(len(a_pool)),
        "b_pool": int(len(b_pool_df)),
    }

    return a_tier, b_tier, liquid_top_n, len(liquid_syms), diagnostics


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

    us_a, us_b, us_topn, us_used, us_diag = run_market("us", us_symbols, cfg)
    hk_a, hk_b, hk_topn, hk_used, hk_diag = run_market("hk", hk_symbols, cfg)

    now = datetime.now().strftime("%Y-%m-%d")
    report_md = [f"# 【策略日报】{now} {cfg['report_time']}", "", "## 港股 A档 Top5（主触发+量能+其余严格条件）"]
    if hk_a.empty:
        report_md.append("- 今日无满足A档条件的港股标的")
    else:
        report_md += [to_line(r) for _, r in hk_a.iterrows()]

    report_md += ["", "## 港股 B档 Top5（主触发+量能，剩余按得分）"]
    if hk_b.empty:
        report_md.append("- 今日无满足B档条件的港股标的")
    else:
        report_md += [to_line(r) for _, r in hk_b.iterrows()]

    report_md += ["", "## 美股 A档 Top5（主触发+量能+其余严格条件）"]
    if us_a.empty:
        report_md.append("- 今日无满足A档条件的美股标的")
    else:
        report_md += [to_line(r) for _, r in us_a.iterrows()]

    report_md += ["", "## 美股 B档 Top5（主触发+量能，剩余按得分）"]
    if us_b.empty:
        report_md.append("- 今日无满足B档条件的美股标的")
    else:
        report_md += [to_line(r) for _, r in us_b.iterrows()]

    report_md += [
        "",
        "## 数据检测点（用于排查为何无结果）",
        f"- 港股：大盘同向={hk_diag['mkt_ok']} | 流动性入池={hk_diag['liquid_selected']} | 成功拉取={hk_diag['fetched_ok']} | 历史不足={hk_diag['insufficient_history']} | 参与评估={hk_diag['evaluated']} | 主触发通过={hk_diag['pass_r1']} | 量能通过={hk_diag['pass_r2']} | 主触发+量能={hk_diag['pass_r1r2']} | eligible通过={hk_diag['pass_eligible']} | A池={hk_diag['a_pool']} | B池={hk_diag['b_pool']}",
        f"- 美股：大盘同向={us_diag['mkt_ok']} | 流动性入池={us_diag['liquid_selected']} | 成功拉取={us_diag['fetched_ok']} | 历史不足={us_diag['insufficient_history']} | 参与评估={us_diag['evaluated']} | 主触发通过={us_diag['pass_r1']} | 量能通过={us_diag['pass_r2']} | 主触发+量能={us_diag['pass_r1r2']} | eligible通过={us_diag['pass_eligible']} | A池={us_diag['a_pool']} | B池={us_diag['b_pool']}",
        "",
        "## 备注",
        "- 数据源: yfinance（免费）",
        "- 美股使用上一个交易日收盘数据；港股使用上一个交易日收盘数据",
        f"- 流动性预筛：按近20日平均成交额（ADV20）筛选，港股Top{hk_topn}（可用{hk_used}），美股Top{us_topn}（可用{us_used}）",
        "- 打分基于你的8条规则，满分100",
    ]

    report_file = BASE / "reports" / "daily" / f"{now}.md"
    report_file.write_text("\n".join(report_md), encoding="utf-8")

    if not us_a.empty:
        us_a.to_csv(BASE / "reports" / "daily" / f"{now}_us_a.csv", index=False)
    if not us_b.empty:
        us_b.to_csv(BASE / "reports" / "daily" / f"{now}_us_b.csv", index=False)
    if not hk_a.empty:
        hk_a.to_csv(BASE / "reports" / "daily" / f"{now}_hk_a.csv", index=False)
    if not hk_b.empty:
        hk_b.to_csv(BASE / "reports" / "daily" / f"{now}_hk_b.csv", index=False)

    print(report_file)


if __name__ == "__main__":
    main()

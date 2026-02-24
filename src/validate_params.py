from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from pathlib import Path
import yaml
import pandas as pd
import yfinance as yf

BASE = Path(__file__).resolve().parents[1]


@dataclass
class Params:
    short: int
    long: int
    vol: float
    atr_min: float


def load_cfg():
    with open(BASE / "config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def signal_frame(df: pd.DataFrame, p: Params):
    d = df.copy()
    d["short_u"] = d["High"].rolling(p.short).max().shift(1)
    d["long_u"] = d["High"].rolling(p.long).max().shift(1)
    d["vol20"] = d["Volume"].rolling(20).mean()
    tr = pd.concat([(d["High"] - d["Low"]), (d["High"] - d["Close"].shift(1)).abs(), (d["Low"] - d["Close"].shift(1)).abs()], axis=1).max(axis=1)
    d["atr_pct"] = tr.rolling(14).mean() / d["Close"]

    breakout = (d["short_u"] > d["long_u"]) & (d["short_u"].shift(1) <= d["long_u"].shift(1))
    vol_ok = (d["Volume"] > p.vol * d["vol20"]) | (d["Volume"].shift(1) > p.vol * d["vol20"].shift(1))
    hold2 = (d["Close"] > d["long_u"]) & (d["Close"].shift(1) > d["long_u"].shift(1))
    atr_ok = d["atr_pct"] >= p.atr_min
    d["entry"] = breakout & vol_ok & hold2 & atr_ok
    d["ret_10"] = d["Close"].shift(-10) / d["Close"] - 1
    return d


def evaluate_universe(symbols: list[str], p: Params):
    rets = []
    for s in symbols:
        try:
            d = yf.download(s, period="5y", interval="1d", auto_adjust=False, progress=False, threads=False)
            if d.empty or len(d) < 300:
                continue
            f = signal_frame(d[["Open", "High", "Low", "Close", "Volume"]].dropna(), p)
            x = f.loc[f["entry"], "ret_10"].dropna()
            if len(x):
                rets.extend(x.tolist())
        except Exception:
            continue
    if not rets:
        return {"trades": 0, "avg_ret_10d": 0.0, "win_rate": 0.0}
    s = pd.Series(rets)
    return {"trades": int(len(s)), "avg_ret_10d": float(s.mean()), "win_rate": float((s > 0).mean())}


def main():
    cfg = load_cfg()
    # lightweight proxy baskets for parameter robustness
    us = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "JPM", "XOM", "UNH", "AVGO"]
    hk = ["0700.HK", "9988.HK", "3690.HK", "1810.HK", "1211.HK", "0005.HK", "1299.HK", "0941.HK"]
    syms = us + hk

    grid = product([18, 20, 22], [50, 55, 60], [1.3, 1.5, 1.8], [0.01, 0.012, 0.015])
    rows = []
    for short, long, vol, atr_min in grid:
        if short >= long:
            continue
        p = Params(short, long, vol, atr_min)
        m = evaluate_universe(syms, p)
        rows.append({"short": short, "long": long, "vol": vol, "atr_min": atr_min, **m})

    out = pd.DataFrame(rows).sort_values(["avg_ret_10d", "win_rate", "trades"], ascending=[False, False, False])
    (BASE / "reports" / "backtest").mkdir(parents=True, exist_ok=True)
    path = BASE / "reports" / "backtest" / "param_scan.csv"
    out.to_csv(path, index=False)
    print(path)


if __name__ == "__main__":
    main()

from __future__ import annotations

import numpy as np
import pandas as pd


def macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = dif - dea
    return dif, dea, hist


def atr(df: pd.DataFrame, period=14):
    prev_close = df["Close"].shift(1)
    tr = pd.concat(
        [
            df["High"] - df["Low"],
            (df["High"] - prev_close).abs(),
            (df["Low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period).mean()


def evaluate_symbol(df: pd.DataFrame, mkt_ok: bool, cfg: dict, weights: dict):
    if len(df) < cfg["min_history_days"]:
        return None

    s, l = cfg["short_channel"], cfg["long_channel"]
    d = df.copy()
    d["short_upper"] = d["High"].rolling(s).max().shift(1)
    d["long_upper"] = d["High"].rolling(l).max().shift(1)
    d["vol20"] = d["Volume"].rolling(20).mean()
    d["atr"] = atr(d, cfg["atr_period"])
    d["atr_pct"] = d["atr"] / d["Close"]
    d["dif"], d["dea"], _ = macd(d["Close"], cfg["macd_fast"], cfg["macd_slow"], cfg["macd_signal"])

    t, y = d.iloc[-1], d.iloc[-2]

    # 主触发改为“状态满足”（短通道在长通道之上），不是仅限当天刚上穿
    r1 = t["short_upper"] > t["long_upper"]
    r2 = (t["Volume"] > cfg["volume_ratio_threshold"] * t["vol20"]) or (
        y["Volume"] > cfg["volume_ratio_threshold"] * y["vol20"]
    )
    r3 = (t["Close"] > t["long_upper"]) and (y["Close"] > y["long_upper"])
    macd_cross = (y["dif"] <= y["dea"]) and (t["dif"] > t["dea"])
    macd_above_zero = (t["dif"] > 0) and (t["dea"] > 0)
    r4 = macd_cross or macd_above_zero

    r5 = t["atr_pct"] >= cfg["atr_pct_min"]
    r6 = bool(mkt_ok)

    # Exit checks should be false for candidates
    r7 = t["Close"] >= t["short_upper"]
    r8 = not (((y["dif"] >= y["dea"]) and (t["dif"] < t["dea"])) and (t["Volume"] < t["vol20"]))

    checks = {
        "rule_1_breakout": r1,
        "rule_2_volume": r2,
        "rule_3_hold_above": r3,
        "rule_4_macd": r4,
        "rule_5_atr_filter": r5,
        "rule_6_trend_filter": r6,
        "rule_7_not_fall_back": r7,
        "rule_8_not_bear_cross_shrink": r8,
    }
    score = sum(weights[k] for k, v in checks.items() if v)

    # Hard filter: must pass filter & exit guards
    eligible = r5 and r6 and r7 and r8
    return {"score": int(score), "eligible": eligible, **checks}

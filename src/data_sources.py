from __future__ import annotations

from io import StringIO
from pathlib import Path
import pandas as pd

NASDAQ_LISTED = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
HKEX_LIST = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"


def _read_pipe_text_url(url: str) -> pd.DataFrame:
    text = pd.read_csv(url, sep="\n", header=None).iloc[:, 0]
    text = text[text.str.contains("\|")]
    body = "\n".join(text.tolist())
    return pd.read_csv(StringIO(body), sep="|")


def get_us_universe(max_symbols: int = 2500) -> list[str]:
    nasdaq = _read_pipe_text_url(NASDAQ_LISTED)
    other = _read_pipe_text_url(OTHER_LISTED)

    n_symbols = nasdaq[(nasdaq["Test Issue"] == "N") & (nasdaq["ETF"] == "N")]["Symbol"].astype(str)
    o_symbols = other[
        (other["Test Issue"] == "N")
        & (~other["Exchange"].astype(str).str.upper().eq("V"))
        & (~other["ETF"].astype(str).str.upper().eq("Y"))
    ]["ACT Symbol"].astype(str)

    symbols = pd.Series(pd.concat([n_symbols, o_symbols], ignore_index=True).unique())
    symbols = symbols[~symbols.str.contains("[\^/\\$]", regex=True)]
    symbols = symbols[symbols.str.fullmatch(r"[A-Z\.\-]{1,7}", na=False)]
    return symbols.sort_values().head(max_symbols).tolist()


def get_hk_universe(max_symbols: int = 2600) -> list[str]:
    # HKEX list has code column in first few columns; robustly detect 4/5 digit stock code.
    df = pd.read_excel(HKEX_LIST, engine="openpyxl")
    flat = pd.DataFrame({"_": df.astype(str).stack().values})
    codes = flat["_"].str.extract(r"\b(\d{4,5})\b", expand=False).dropna().unique().tolist()
    symbols = [f"{c.zfill(4)}.HK" for c in codes]
    return sorted(list(dict.fromkeys(symbols)))[:max_symbols]


def ensure_dirs(base: Path) -> None:
    for p in ["data", "reports", "reports/daily", "reports/backtest"]:
        (base / p).mkdir(parents=True, exist_ok=True)

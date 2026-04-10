#!/usr/bin/env python3
"""Spread visualization app for arbitrary exchange-symbol pairs."""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytz
from flask import Flask, jsonify, render_template, request


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
app = Flask(__name__, template_folder=TEMPLATE_DIR)

DB_DIR = os.path.join(PROJECT_ROOT, "logs")
EXCHANGE_DB_MAP = {
    "omni": os.path.join(DB_DIR, "exchange_bbo.db"),
    "trade_xyz": os.path.join(DB_DIR, "exchange_bbo.db"),
    "lighter": os.path.join(DB_DIR, "exchange_bbo.db"),
}

TZ_UTC8 = pytz.timezone("Asia/Shanghai")


def get_available_symbols(exchange: str) -> List[str]:
    symbols = set()
    db_path = EXCHANGE_DB_MAP["omni"]
    try:
        if not os.path.exists(db_path):
            return []
        conn = sqlite3.connect(db_path, timeout=10.0)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT symbol FROM bbo_data WHERE exchange = ? ORDER BY symbol",
            (exchange,),
        )
        symbols.update(row[0] for row in cursor.fetchall() if row and row[0])
        conn.close()
    except Exception as exc:
        print(f"Error reading symbols from {db_path}: {exc}")
    return sorted(symbols)


def get_exchange_options() -> List[str]:
    return list(EXCHANGE_DB_MAP.keys())


def query_bbo_data(exchange: str, symbol: str, start_time: Optional[str], end_time: Optional[str]) -> pd.DataFrame:
    db_path = EXCHANGE_DB_MAP.get(exchange)
    if not db_path or not os.path.exists(db_path):
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(db_path, timeout=10.0)
        query = (
            "SELECT timestamp, best_bid, best_ask, best_bid_size, best_ask_size "
            "FROM bbo_data WHERE exchange = ? AND symbol = ?"
        )
        params = [exchange, symbol]
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        query += " ORDER BY timestamp ASC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df["best_bid"] = pd.to_numeric(df["best_bid"], errors="coerce")
        df["best_ask"] = pd.to_numeric(df["best_ask"], errors="coerce")
        df["best_bid_size"] = pd.to_numeric(df["best_bid_size"], errors="coerce")
        df["best_ask_size"] = pd.to_numeric(df["best_ask_size"], errors="coerce")
        return df
    except Exception as exc:
        print(f"Error querying {exchange}: {exc}")
        return pd.DataFrame()


def merge_and_resample_data(df1: pd.DataFrame, df2: pd.DataFrame, exchange1: str, exchange2: str) -> pd.DataFrame:
    if df1.empty and df2.empty:
        return pd.DataFrame()

    if not df1.empty:
        df1 = df1.copy().set_index("timestamp").add_prefix(f"{exchange1}_")
    if not df2.empty:
        df2 = df2.copy().set_index("timestamp").add_prefix(f"{exchange2}_")

    if not df1.empty and not df2.empty:
        merged = pd.merge(df1, df2, left_index=True, right_index=True, how="outer", sort=True)
        price_cols = [col for col in merged.columns if "best_bid" in col or "best_ask" in col]
        for col in price_cols:
            merged[col] = merged[col].ffill(limit=3)
    elif not df1.empty:
        merged = df1
    else:
        merged = df2

    return merged.reset_index()


def calculate_spreads(df: pd.DataFrame, exchange1: str, exchange2: str) -> pd.DataFrame:
    df = df.copy()
    prefix1 = f"{exchange1}_"
    prefix2 = f"{exchange2}_"

    df["spread_1"] = np.nan
    df["spread_2"] = np.nan

    bid_col2 = f"{prefix2}best_bid"
    ask_col1 = f"{prefix1}best_ask"
    bid_col1 = f"{prefix1}best_bid"
    ask_col2 = f"{prefix2}best_ask"

    if bid_col2 in df.columns and ask_col1 in df.columns:
        mask1 = df[bid_col2].notna() & df[ask_col1].notna() & (df[ask_col1] != 0)
        if mask1.any():
            df.loc[mask1, "spread_1"] = (
                (df.loc[mask1, bid_col2] - df.loc[mask1, ask_col1]) / df.loc[mask1, ask_col1]
            )

    if bid_col1 in df.columns and ask_col2 in df.columns:
        mask2 = df[bid_col1].notna() & df[ask_col2].notna() & (df[ask_col2] != 0)
        if mask2.any():
            df.loc[mask2, "spread_2"] = (
                (df.loc[mask2, bid_col1] - df.loc[mask2, ask_col2]) / df.loc[mask2, ask_col2]
            )

    return df


def _normalize_time_param(value: Optional[str], default_hours: int) -> tuple[str, str]:
    if not value:
        end_dt = datetime.now(TZ_UTC8)
        start_dt = end_dt - timedelta(hours=default_hours)
        return (
            start_dt.replace(tzinfo=None).isoformat(),
            end_dt.replace(tzinfo=None).isoformat(),
        )
    raise RuntimeError("_normalize_time_param should not be called with explicit value")


def _convert_to_utc8_naive(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.astimezone(TZ_UTC8).replace(tzinfo=None).isoformat()
        if "+" in value or value.count("-") > 2:
            dt = datetime.fromisoformat(value)
            return dt.astimezone(TZ_UTC8).replace(tzinfo=None).isoformat()
        return value
    except Exception:
        return value


@app.route("/")
def index():
    exchange_options = get_exchange_options()
    default_exchange1 = "omni"
    default_exchange2 = "trade_xyz"
    exchange1_symbols = get_available_symbols(default_exchange1) or ["XAU"]
    exchange2_symbols = get_available_symbols(default_exchange2) or ["XAU"]
    default_end = datetime.now()
    default_start = default_end - timedelta(hours=24)
    return render_template(
        "frontend_spread.html",
        exchange_options=exchange_options,
        default_exchange1=default_exchange1,
        default_exchange2=default_exchange2,
        default_symbol1=exchange1_symbols[0],
        default_symbol2=exchange2_symbols[0],
        exchange1_symbols=exchange1_symbols,
        exchange2_symbols=exchange2_symbols,
        default_start=default_start.strftime("%Y-%m-%dT%H:%M"),
        default_end=default_end.strftime("%Y-%m-%dT%H:%M"),
    )


@app.route("/api/symbols", methods=["GET"])
def get_symbols():
    exchange = request.args.get("exchange", "omni").strip().lower()
    if exchange not in EXCHANGE_DB_MAP:
        return jsonify({"success": False, "message": f"Unsupported exchange: {exchange}"})
    symbols = get_available_symbols(exchange)
    return jsonify({"success": True, "symbols": symbols})


@app.route("/api/spread_data", methods=["GET"])
def get_spread_data():
    exchange1 = request.args.get("exchange1", "omni").strip().lower()
    exchange2 = request.args.get("exchange2", "trade_xyz").strip().lower()
    symbol1 = request.args.get("symbol1", "XAU").strip().upper()
    symbol2 = request.args.get("symbol2", "XAU").strip().upper()
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    if exchange1 not in EXCHANGE_DB_MAP or exchange2 not in EXCHANGE_DB_MAP:
        return jsonify({"success": False, "message": "Unsupported exchange"})
    if not start_time:
        start_time, end_time = _normalize_time_param(None, default_hours=24)
    else:
        start_time = _convert_to_utc8_naive(start_time)
        end_time = _convert_to_utc8_naive(end_time)

    df1 = query_bbo_data(exchange1, symbol1, start_time, end_time)
    df2 = query_bbo_data(exchange2, symbol2, start_time, end_time)
    merged_df = merge_and_resample_data(df1, df2, exchange1, exchange2)

    if merged_df.empty:
        return jsonify({"success": False, "message": "No data available for the selected time range"})

    spread_df = calculate_spreads(merged_df, exchange1, exchange2)
    timestamps = spread_df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist()

    result: Dict[str, object] = {
        "success": True,
        "exchange1": exchange1,
        "exchange2": exchange2,
        "symbol1": symbol1,
        "symbol2": symbol2,
        "spread_1_label": f"{exchange2} bid - {exchange1} ask",
        "spread_2_label": f"{exchange1} bid - {exchange2} ask",
        "data_points": len(spread_df),
        "timestamps": timestamps,
        "spread_1": spread_df["spread_1"].replace({np.nan: None}).tolist(),
        "spread_2": spread_df["spread_2"].replace({np.nan: None}).tolist(),
        "exchange1_timestamps": df1["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist() if not df1.empty else [],
        "exchange1_bid": df1["best_bid"].replace({np.nan: None}).tolist() if not df1.empty else [],
        "exchange1_ask": df1["best_ask"].replace({np.nan: None}).tolist() if not df1.empty else [],
        "exchange2_timestamps": df2["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S").tolist() if not df2.empty else [],
        "exchange2_bid": df2["best_bid"].replace({np.nan: None}).tolist() if not df2.empty else [],
        "exchange2_ask": df2["best_ask"].replace({np.nan: None}).tolist() if not df2.empty else [],
    }

    for spread_key in ("spread_1", "spread_2"):
        series = spread_df[spread_key].dropna()
        if not series.empty:
            result[f"{spread_key}_stats"] = {
                "mean": float(series.mean() * 100),
                "median": float(series.median() * 100),
                "max": float(series.max() * 100),
                "min": float(series.min() * 100),
                "std": float(series.std() * 100),
            }

    return jsonify(result)


if __name__ == "__main__":
    os.makedirs(TEMPLATE_DIR, exist_ok=True)
    print("frontend spread monitor: http://localhost:8710")
    app.run(host="0.0.0.0", port=8710, debug=False)

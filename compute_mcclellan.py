"""
McClellan Oscillator & Summation Index — NASDAQ
Pulls daily snapshots from Alpaca, computes breadth, writes JSON.
"""

import json
import os
import datetime
import urllib.request
import urllib.error

ALPACA_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")
BASE_URL = "https://data.alpaca.markets"
HISTORY_FILE = "mcclellan_history.json"
OUTPUT_FILE = "mcclellan_latest.json"
EXCHANGE = "NASDAQ"
EMA_FAST = 19
EMA_SLOW = 39


def alpaca_get(url):
    req = urllib.request.Request(url)
    req.add_header("APCA-API-KEY-ID", ALPACA_KEY)
    req.add_header("APCA-API-SECRET-KEY", ALPACA_SECRET)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def get_nasdaq_tickers():
    tickers = []
    url = "https://paper-api.alpaca.markets/v2/assets?status=active&exchange=NASDAQ&asset_class=us_equity"
    data = alpaca_get(url)
    for asset in data:
        if asset.get("tradable") and asset.get("status") == "active":
            sym = asset.get("symbol", "")
            if sym and len(sym) <= 5 and not any(c in sym for c in ["/", ".", "-"]):
                tickers.append(sym)
    return tickers


def get_snapshots_batch(tickers):
    all_snapshots = {}
    batch_size = 199
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        symbols = ",".join(batch)
        url = f"{BASE_URL}/v2/stocks/snapshots?symbols={symbols}&feed=iex"
        try:
            data = alpaca_get(url)
            if isinstance(data, dict):
                all_snapshots.update(data)
        except Exception as e:
            print(f"  Batch {i // batch_size + 1} error: {e}")
    return all_snapshots


def count_advances_declines(snapshots):
    advances = declines = unchanged = total = 0
    for sym, snap in snapshots.items():
        try:
            close_today = snap.get("dailyBar", {}).get("c", 0)
            close_prev = snap.get("prevDailyBar", {}).get("c", 0)
            if close_today <= 0 or close_prev <= 0:
                continue
            total += 1
            if close_today > close_prev: advances += 1
            elif close_today < close_prev: declines += 1
            else: unchanged += 1
        except (KeyError, TypeError):
            continue
    return advances, declines, unchanged, total


def ema(current_value, prev_ema, period):
    k = 2.0 / (period + 1.0)
    return current_value * k + prev_ema * (1.0 - k)


def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    return {"days": [], "ema_fast": None, "ema_slow": None, "summation_index": 0}


def save_history(history):
    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=2)


def main():
    today = datetime.date.today().isoformat()
    print(f"=== McClellan Breadth — {today} ===")

    if not ALPACA_KEY or not ALPACA_SECRET:
        print("ERROR: ALPACA_API_KEY and ALPACA_SECRET_KEY must be set")
        return

    print("Fetching NASDAQ tickers...")
    tickers = get_nasdaq_tickers()
    print(f"  Found {len(tickers)} active tickers")

    print("Fetching snapshots...")
    snapshots = get_snapshots_batch(tickers)
    print(f"  Got {len(snapshots)} snapshots")

    advances, declines, unchanged, total = count_advances_declines(snapshots)
    net_advances = advances - declines
    print(f"  Adv: {advances} | Dec: {declines} | Net: {net_advances} | Total: {total}")

    history = load_history()
    if history["days"] and history["days"][-1].get("date") == today:
        print(f"  Already computed for {today}, skipping.")
        return

    ema_f = history.get("ema_fast")
    ema_s = history.get("ema_slow")
    si = history.get("summation_index", 0)

    if ema_f is None:
        ema_f = float(net_advances)
        ema_s = float(net_advances)
    else:
        ema_f = ema(net_advances, ema_f, EMA_FAST)
        ema_s = ema(net_advances, ema_s, EMA_SLOW)

    mco = ema_f - ema_s
    si = si + mco

    mco_direction = "flat"
    if history["days"]:
        prev_mco = history["days"][-1].get("mco", 0)
        if mco > prev_mco + 0.5: mco_direction = "rising"
        elif mco < prev_mco - 0.5: mco_direction = "falling"

    print(f"  MCO: {mco:.2f} | MSI: {si:.2f} | Dir: {mco_direction}")

    day_entry = {
        "date": today, "advances": advances, "declines": declines,
        "unchanged": unchanged, "total": total, "net_advances": net_advances,
        "mco": round(mco, 2), "msi": round(si, 2),
        "ema_fast": round(ema_f, 4), "ema_slow": round(ema_s, 4),
        "mco_direction": mco_direction,
    }

    history["days"].append(day_entry)
    if len(history["days"]) > 100:
        history["days"] = history["days"][-100:]
    history["ema_fast"] = ema_f
    history["ema_slow"] = ema_s
    history["summation_index"] = si
    save_history(history)

    if si > 0 and mco > 0: msi_zone = "bullish"
    elif si > 0: msi_zone = "weakening"
    elif mco > 0: msi_zone = "recovering"
    else: msi_zone = "bearish"

    breadth_power = round((net_advances / total * 100), 1) if total > 0 else 0

    output = {
        "updated": today, "exchange": EXCHANGE,
        "current": {
            "mco": round(mco, 2), "msi": round(si, 2),
            "mco_direction": mco_direction, "msi_zone": msi_zone,
            "advances": advances, "declines": declines,
            "unchanged": unchanged, "total": total,
            "net_advances": net_advances, "breadth_power": breadth_power,
        },
        "history": [{"date": d["date"], "mco": d["mco"], "msi": d["msi"],
                      "net_advances": d["net_advances"], "advances": d["advances"],
                      "declines": d["declines"]} for d in history["days"][-30:]],
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"Output written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

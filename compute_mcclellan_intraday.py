"""
McClellan Intraday — NASDAQ
Long-running script that polls Alpaca every 15 minutes during market hours.
Computes intraday MCO/MSI based on current snapshots vs previous close.
Writes mcclellan_intraday.json which the dashboard fetches.

Designed to run as a single long GitHub Actions job that starts at market open
and runs until market close.
"""

import json
import os
import time
import datetime
import urllib.request
import urllib.error

# ── CONFIG ──
ALPACA_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")
BASE_URL = "https://data.alpaca.markets"
INTRADAY_FILE = "mcclellan_intraday.json"
HISTORY_FILE = "mcclellan_history.json"  # Read-only, used to seed EMAs
EXCHANGE = "NASDAQ"

POLL_INTERVAL = 15 * 60  # 15 minutes in seconds
EMA_FAST = 19
EMA_SLOW = 39

# Market hours in UTC (EST is UTC-5, EDT is UTC-4)
# Market: 9:30 AM ET = 14:30 UTC (EDT) or 13:30 UTC (EST)
# Close:  4:00 PM ET = 21:00 UTC (EDT) or 20:00 UTC (EST)
# We'll use EDT for now (most of the year), script handles both via timezone


def alpaca_get(url):
    req = urllib.request.Request(url)
    req.add_header("APCA-API-KEY-ID", ALPACA_KEY)
    req.add_header("APCA-API-SECRET-KEY", ALPACA_SECRET)
    req.add_header("Accept", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def get_nasdaq_tickers():
    """Get all active NASDAQ-listed common stock tickers from Alpaca."""
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
    """Fetch snapshots in batches of 199 tickers."""
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
        except urllib.error.HTTPError as e:
            print(f"  Batch {i // batch_size + 1} HTTP error: {e.code}")
        except Exception as e:
            print(f"  Batch {i // batch_size + 1} exception: {e}")
    return all_snapshots


def count_advances_declines(snapshots):
    """Count advancing vs declining stocks based on intraday price vs prev close."""
    advances = declines = unchanged = total = 0
    for sym, snap in snapshots.items():
        try:
            # Use latest trade price if available, fall back to daily bar close
            latest_trade = snap.get("latestTrade", {})
            current_price = latest_trade.get("p", 0) if latest_trade else 0

            # If no latest trade, fall back to daily bar
            if current_price <= 0:
                daily = snap.get("dailyBar", {})
                current_price = daily.get("c", 0) if daily else 0

            prev = snap.get("prevDailyBar", {})
            prev_close = prev.get("c", 0) if prev else 0

            if current_price <= 0 or prev_close <= 0:
                continue

            total += 1
            if current_price > prev_close:
                advances += 1
            elif current_price < prev_close:
                declines += 1
            else:
                unchanged += 1
        except (KeyError, TypeError):
            continue
    return advances, declines, unchanged, total


def ema(current_value, prev_ema, period):
    k = 2.0 / (period + 1.0)
    return current_value * k + prev_ema * (1.0 - k)


def load_daily_history():
    """Load the daily history file to seed EMAs from yesterday's values."""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            return json.load(f)
    return None


def is_market_open_utc():
    """Check if NASDAQ is open right now (UTC time)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    # Skip weekends
    if now.weekday() >= 5:  # Sat=5, Sun=6
        return False
    # 13:30 UTC to 20:00 UTC covers both EST (winter) and EDT (summer) market hours
    # EST: 9:30-16:00 ET = 14:30-21:00 UTC
    # EDT: 9:30-16:00 ET = 13:30-20:00 UTC
    # Use the wider window 13:30-21:00 UTC to be safe
    hour_min = now.hour * 60 + now.minute
    return 13 * 60 + 30 <= hour_min <= 21 * 60


def compute_breadth_snapshot(tickers):
    """Single snapshot computation."""
    print(f"  Fetching snapshots for {len(tickers)} tickers...")
    snapshots = get_snapshots_batch(tickers)
    print(f"  Got {len(snapshots)} snapshots")

    advances, declines, unchanged, total = count_advances_declines(snapshots)
    net_advances = advances - declines

    if total == 0:
        print("  WARNING: No valid snapshots, skipping")
        return None

    # Seed EMAs from daily history
    daily_hist = load_daily_history()
    if daily_hist and daily_hist.get("ema_fast") is not None:
        ema_f = daily_hist["ema_fast"]
        ema_s = daily_hist["ema_slow"]
        prev_si = daily_hist.get("summation_index", 0)
        # Compute today's projected EMAs based on current net advances
        today_ema_f = ema(net_advances, ema_f, EMA_FAST)
        today_ema_s = ema(net_advances, ema_s, EMA_SLOW)
        mco = today_ema_f - today_ema_s
        # Projected MSI = previous SI + today's projected MCO
        si = prev_si + mco
    else:
        # No history yet — first day, estimate from current
        mco = 0
        si = 0
        today_ema_f = float(net_advances)
        today_ema_s = float(net_advances)

    breadth_power = round((net_advances / total * 100), 1) if total > 0 else 0

    if si > 0 and mco > 0:
        msi_zone = "bullish"
    elif si > 0:
        msi_zone = "weakening"
    elif mco > 0:
        msi_zone = "recovering"
    else:
        msi_zone = "bearish"

    return {
        "advances": advances,
        "declines": declines,
        "unchanged": unchanged,
        "total": total,
        "net_advances": net_advances,
        "mco": round(mco, 2),
        "msi": round(si, 2),
        "ema_fast": round(today_ema_f, 4),
        "ema_slow": round(today_ema_s, 4),
        "breadth_power": breadth_power,
        "msi_zone": msi_zone,
    }


def write_intraday(snapshot, intraday_history):
    """Write the intraday snapshot to JSON."""
    today = datetime.date.today().isoformat()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Determine direction from history
    mco_direction = "flat"
    if intraday_history and len(intraday_history) > 0:
        prev_mco = intraday_history[-1].get("mco", 0)
        if snapshot["mco"] > prev_mco + 0.5:
            mco_direction = "rising"
        elif snapshot["mco"] < prev_mco - 0.5:
            mco_direction = "falling"

    output = {
        "updated": today,
        "updated_time": now,
        "exchange": EXCHANGE,
        "is_intraday": True,
        "current": {
            "mco": snapshot["mco"],
            "msi": snapshot["msi"],
            "mco_direction": mco_direction,
            "msi_zone": snapshot["msi_zone"],
            "advances": snapshot["advances"],
            "declines": snapshot["declines"],
            "unchanged": snapshot["unchanged"],
            "total": snapshot["total"],
            "net_advances": snapshot["net_advances"],
            "breadth_power": snapshot["breadth_power"],
        },
        "intraday_history": intraday_history,
    }

    with open(INTRADAY_FILE, "w") as f:
        json.dump(output, f, indent=2)


def upload_via_api(filename):
    """Upload file to GitHub via API (bypasses git push)."""
    gh_token = os.environ.get("GITHUB_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not gh_token or not repo:
        print("  GITHUB_TOKEN/GITHUB_REPOSITORY missing, skipping upload")
        return False

    import base64
    with open(filename, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode()

    api_url = f"https://api.github.com/repos/{repo}/contents/{filename}"

    # Get current SHA
    sha = ""
    try:
        req = urllib.request.Request(api_url)
        req.add_header("Authorization", f"token {gh_token}")
        with urllib.request.urlopen(req, timeout=30) as resp:
            existing = json.loads(resp.read().decode())
            sha = existing.get("sha", "")
    except Exception:
        pass

    # Build payload
    payload = {
        "message": f"📊 Intraday update {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
        "content": content_b64,
    }
    if sha:
        payload["sha"] = sha

    req = urllib.request.Request(api_url, method="PUT")
    req.add_header("Authorization", f"token {gh_token}")
    req.add_header("Content-Type", "application/json")
    req.data = json.dumps(payload).encode()

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            return result.get("content", {}).get("sha", None) is not None
    except urllib.error.HTTPError as e:
        print(f"  Upload failed: HTTP {e.code} {e.read().decode()}")
        return False


def main():
    print("=== McClellan Intraday Poller ===")

    if not ALPACA_KEY or not ALPACA_SECRET:
        print("ERROR: ALPACA_API_KEY and ALPACA_SECRET_KEY must be set")
        return

    print("Fetching NASDAQ ticker list (one-time)...")
    tickers = get_nasdaq_tickers()
    print(f"Tracking {len(tickers)} tickers")

    intraday_history = []
    poll_count = 0
    max_polls = 30  # Safety limit: 30 polls * 15 min = 7.5 hours max

    while poll_count < max_polls:
        if not is_market_open_utc():
            print(f"\nMarket closed at {datetime.datetime.now(datetime.timezone.utc).isoformat()}, exiting")
            break

        poll_count += 1
        now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")
        print(f"\n--- Poll #{poll_count} at {now_str} ---")

        snapshot = compute_breadth_snapshot(tickers)
        if snapshot:
            print(f"  MCO: {snapshot['mco']} | MSI: {snapshot['msi']}")
            print(f"  Adv: {snapshot['advances']} | Dec: {snapshot['declines']} | BP: {snapshot['breadth_power']}")

            # Append to intraday history
            history_entry = {
                "time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "mco": snapshot["mco"],
                "msi": snapshot["msi"],
                "net_advances": snapshot["net_advances"],
                "advances": snapshot["advances"],
                "declines": snapshot["declines"],
                "breadth_power": snapshot["breadth_power"],
            }
            intraday_history.append(history_entry)
            # Keep last 30 entries (last 7.5 hours)
            if len(intraday_history) > 30:
                intraday_history = intraday_history[-30:]

            # Write & upload
            write_intraday(snapshot, intraday_history)
            uploaded = upload_via_api(INTRADAY_FILE)
            print(f"  Upload: {'OK' if uploaded else 'FAILED'}")

        # Sleep until next poll
        if poll_count < max_polls:
            print(f"  Sleeping {POLL_INTERVAL // 60} minutes until next poll...")
            time.sleep(POLL_INTERVAL)

    print(f"\nDone. Total polls: {poll_count}")


if __name__ == "__main__":
    main()

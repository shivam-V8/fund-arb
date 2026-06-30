#!/usr/bin/env python3
"""
funding_arb.py - Funding-rate arbitrage scanner for tokenized-stock perps.

Compares per-hour funding rates between:
  - Hyperliquid HIP-3 "xyz" dex (TradeXYZ / Unit)  -> symbols like xyz:NVDA
  - Ondo Perps                                      -> markets like NVDA-USD.P

Both venues settle funding HOURLY and quote a per-hour rate using the same
"8h rate paid in hourly installments" convention, so the rates are directly
comparable. Annualised = hourly_rate * 24 * 365.

A delta-neutral funding capture: SHORT the venue with the higher (more positive)
funding, LONG the venue with the lower funding. You receive the spread each hour
while holding offsetting price exposure.

Needs zero third-party packages. Two unauthenticated HTTP calls per refresh.

Usage:
  python funding_arb.py                     # one-shot table
  python funding_arb.py --watch 30          # refresh every 30s
  python funding_arb.py --min-annual 20     # only show spreads >= 20% APR
  python funding_arb.py --json              # machine-readable
  python funding_arb.py --csv out.csv       # append snapshot to CSV
  python funding_arb.py --serve 8787        # serve the live dashboard
  python funding_arb.py --mock              # run on bundled sample data (offline)
"""

import argparse
import concurrent.futures
import csv as csvmod
import json
import os
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
ONDO_CONTRACTS_URL = "https://api.ondoperps.xyz/v1/perps/contracts"
ONDO_HIST_URL = "https://api.ondoperps.xyz/v1/perps/funding_rate_history"
DEFAULT_DEX = "xyz"                 # TradeXYZ HIP-3 perp dex on Hyperliquid
HOURS_PER_YEAR = 24 * 365          # 8760
AVG24_WINDOW_H = 24                 # short trailing window for the funding average
AVG7D_WINDOW_H = 24 * 7            # long trailing window (168h). One 7d fetch
                                   # yields both windows: 7d uses all of it, 24h
                                   # uses the last 24 samples. Well within the
                                   # per-request caps (HL 500 recs, Ondo 1000).
HTTP_TIMEOUT = 15
USER_AGENT = "funding-arb/1.0 (+stdlib)"

# Hyperliquid HIP-3 taker fee assumption (fraction of notional, per leg).
# HIP-3 base taker is ~2x native (~9 bps). trade.xyz "growth mode" markets can
# be far cheaper. Override with --hl-taker. Ondo's taker fee comes from its API.
DEFAULT_HL_TAKER = 0.00045

# Map an HL base symbol -> the canonical symbol used to match against Ondo.
# Commodities are quoted differently on each venue but share the same underlying.
HL_TO_CANON = {
    "GOLD": "XAU",
    "SILVER": "XAG",
    "OIL": "WTI",
    "CRUDE": "WTI",
}

# Same-underlying check we deliberately REFUSE to treat as a clean pair.
# HL's XYZ100 is a synthetic Nasdaq-100-style index; Ondo's QQQ is the ETF.
# Related, but not the same instrument -> not a delta-neutral pair. Excluded.
EXCLUDE_CANON = {"XYZ100"}


# ----------------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------------

def _http_json(url, method="GET", body=None):
    data = None
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _to_float(x, default=None):
    try:
        if x is None or x == "":
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def _first_float(d, keys, default=None):
    """First parseable value among candidate keys (handles live field renames)."""
    for k in keys:
        v = _to_float(d.get(k))
        if v is not None:
            return v
    return default


# ----------------------------------------------------------------------------
# Venue fetchers -> {canonical_symbol: {...}}
# ----------------------------------------------------------------------------

def fetch_hyperliquid(dex=DEFAULT_DEX):
    """Returns {canon: {funding_hr, mark, oracle, oi_usd, raw_name}}."""
    payload = _http_json(HL_INFO_URL, method="POST",
                         body={"type": "metaAndAssetCtxs", "dex": dex})
    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("Unexpected Hyperliquid response shape")
    meta, ctxs = payload
    universe = meta.get("universe", [])
    out = {}
    for asset, ctx in zip(universe, ctxs):
        name = asset.get("name", "")            # e.g. "xyz:NVDA"
        base = name.split(":", 1)[1] if ":" in name else name
        canon = HL_TO_CANON.get(base, base)
        funding = _to_float(ctx.get("funding"))
        if funding is None:
            continue
        mark = _first_float(ctx, ["markPx", "midPx", "oraclePx"])
        oi_coins = _first_float(ctx, ["openInterest"], 0.0)
        out[canon] = {
            "funding_hr": funding,
            "mark": _to_float(ctx.get("markPx")) or mark,
            "oracle": _to_float(ctx.get("oraclePx")),
            "oi_coins": oi_coins,
            "oi_usd": (oi_coins * mark) if (mark is not None) else None,
            "raw_name": name,
        }
    return out


def fetch_ondo():
    """Returns {canon: {funding_hr, funding_last, mark, oi_usd, taker, maker, raw_name}}."""
    payload = _http_json(ONDO_CONTRACTS_URL, method="GET")
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, list):
        raise ValueError("Unexpected Ondo response shape")
    out = {}
    for c in result:
        if c.get("disabled"):
            continue
        base = c.get("baseCurrency") or ""
        canon = HL_TO_CANON.get(base, base)     # harmless; Ondo already uses XAU/XAG
        # nextFundingRate = estimate for the current (forward) interval.
        funding = _to_float(c.get("nextFundingRate"))
        if funding is None:
            funding = _to_float(c.get("fundingRate"))
        if funding is None:
            continue
        mark = _first_float(c, ["lastPrice", "indexPrice", "markPrice"])
        # Prefer a USD OI field; otherwise derive from base-currency OI x price.
        oi_usd = _first_float(c, ["openInterestUsd", "openInterestUSD", "oiUsd"])
        oi_base = _first_float(c, ["openInterest", "openInterestBase"])
        if oi_usd is None and oi_base is not None and mark is not None:
            oi_usd = oi_base * mark
        out[canon] = {
            "funding_hr": funding,
            "funding_last": _to_float(c.get("fundingRate")),
            "mark": mark,
            "oi_base": oi_base,
            "oi_usd": oi_usd,
            "taker": _to_float(c.get("takerFee"), 0.0005),
            "maker": _to_float(c.get("makerFee"), 0.0002),
            "raw_name": c.get("market"),
        }
    return out


def _hl_series(coin_raw, lookback_h):
    """Realized hourly funding for an HL coin -> sorted [(time_ms, rate), ...].

    HL returns up to 500 records FORWARD from startTime, so the trailing window
    must be <= ~500h; 7d (168) is comfortably inside that cap."""
    start = int((time.time() - lookback_h * 3600) * 1000)
    data = _http_json(HL_INFO_URL, method="POST",
                     body={"type": "fundingHistory", "coin": coin_raw,
                           "startTime": start})
    out = []
    for d in (data or []):
        t = _to_float(d.get("time"))
        r = _to_float(d.get("fundingRate"))
        if t is not None and r is not None:
            out.append((t, r))
    out.sort()
    return out


def _ondo_series(market_raw, lookback_h):
    """Realized hourly funding for an Ondo market -> sorted [(time_ms, rate), ...].

    Ondo caps at 1000 records and stamps each row with an ISO-8601 `time`
    string (not epoch ms), so parse it explicitly."""
    start = int((time.time() - lookback_h * 3600) * 1000)
    url = ONDO_HIST_URL + "?" + urllib.parse.urlencode(
        {"market": market_raw, "startTime": start, "limit": 1000})
    payload = _http_json(url, method="GET")
    res = payload.get("result") if isinstance(payload, dict) else None
    out = []
    for d in (res or []):
        r = _to_float(d.get("fundingRate"))
        if r is None:
            continue
        ts = d.get("time") or d.get("fundingTime") or d.get("createdAt")
        try:
            t = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp() * 1000
        except (TypeError, ValueError):
            t = None
        if t is not None:
            out.append((t, r))
    out.sort()
    return out


def _mean_window(series, window_h, now_ms):
    """Mean of the rates in `series` whose timestamp is within the trailing
    window_h hours of now_ms. -> (avg, n)."""
    cutoff = now_ms - window_h * 3600 * 1000
    rates = [r for (t, r) in series if t >= cutoff]
    return (sum(rates) / len(rates), len(rates)) if rates else (None, 0)


def _avg_hl_funding(coin_raw, window_h):
    """Mean of realized hourly funding for an HL coin over the window. -> (avg, n)."""
    return _mean_window(_hl_series(coin_raw, window_h), window_h, int(time.time() * 1000))


def _avg_ondo_funding(market_raw, window_h):
    """Mean of realized hourly funding for an Ondo market over the window. -> (avg, n)."""
    return _mean_window(_ondo_series(market_raw, window_h), window_h, int(time.time() * 1000))


def apply_funding_avg(hl, ondo, window_h, mock=False):
    """Replace funding_hr on paired coins with the trailing-window average.

    Price / OI / fees are left untouched (those stay live). On any per-coin
    failure the instantaneous rate is kept, so the snapshot never crashes."""
    paired = (set(hl) & set(ondo)) - EXCLUDE_CANON
    for canon in paired:
        if mock:
            # Offline demo: use the bundled avg24 field if present.
            for venue in (hl[canon], ondo[canon]):
                if "avg24" in venue:
                    venue["funding_hr"] = venue["avg24"]
                    venue["funding_src"] = "24h avg"
                    venue["samples"] = 24
            continue
        try:
            avg, n = _avg_hl_funding(hl[canon]["raw_name"], window_h)
            if avg is not None:
                hl[canon]["funding_hr"] = avg
                hl[canon]["funding_src"] = "avg"
                hl[canon]["samples"] = n
        except (urllib.error.URLError, ValueError, TimeoutError):
            pass
        try:
            avg, n = _avg_ondo_funding(ondo[canon]["raw_name"], window_h)
            if avg is not None:
                ondo[canon]["funding_hr"] = avg
                ondo[canon]["funding_src"] = "avg"
                ondo[canon]["samples"] = n
        except (urllib.error.URLError, ValueError, TimeoutError):
            pass


def attach_funding_averages(hl, ondo, mock=False, max_workers=6):
    """Attach BOTH a 24h and a 7d trailing average of hourly funding to paired
    coins, stored in SEPARATE fields (avg24_hr/avg24_n, avg7d_hr/avg7d_n) so the
    live instantaneous rate is preserved and shown alongside them.

    Each venue is fetched ONCE over the longer (7d) window; the 24h average is
    derived from the last 24 samples of that same series, so adding the 7d view
    costs no extra HTTP calls. Fetches run concurrently; any per-coin failure
    simply leaves that coin without averages (the UI shows '—')."""
    paired = (set(hl) & set(ondo)) - EXCLUDE_CANON

    if mock:
        for canon in paired:
            for venue in (hl[canon], ondo[canon]):
                if "avg24" in venue:
                    venue["avg24_hr"] = venue["avg24"]
                    venue["avg24_n"] = AVG24_WINDOW_H
                if "avg7d" in venue or "avg24" in venue:
                    venue["avg7d_hr"] = venue.get("avg7d", venue.get("avg24"))
                    venue["avg7d_n"] = AVG7D_WINDOW_H
        return

    now_ms = int(time.time() * 1000)

    def task(venue, canon):
        fetch = _hl_series if venue == "hl" else _ondo_series
        store = hl[canon] if venue == "hl" else ondo[canon]
        try:
            series = fetch(store["raw_name"], AVG7D_WINDOW_H)
        except (urllib.error.URLError, ValueError, TimeoutError):
            series = []
        a24, n24 = _mean_window(series, AVG24_WINDOW_H, now_ms)
        a7d, n7d = _mean_window(series, AVG7D_WINDOW_H, now_ms)
        return venue, canon, a24, n24, a7d, n7d

    jobs = [(v, c) for c in paired for v in ("hl", "ondo")]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        for venue, canon, a24, n24, a7d, n7d in ex.map(lambda a: task(*a), jobs):
            store = hl[canon] if venue == "hl" else ondo[canon]
            if a24 is not None:
                store["avg24_hr"] = a24
                store["avg24_n"] = n24
            if a7d is not None:
                store["avg7d_hr"] = a7d
                store["avg7d_n"] = n7d


# ----------------------------------------------------------------------------
# Spread computation
# ----------------------------------------------------------------------------

def compute_spreads(hl, ondo, hl_taker=DEFAULT_HL_TAKER):
    """Intersect venues and build ranked spread rows."""
    rows = []
    for canon in sorted(set(hl) & set(ondo)):
        if canon in EXCLUDE_CANON:
            continue
        h = hl[canon]
        o = ondo[canon]
        f_hl = h["funding_hr"]
        f_ondo = o["funding_hr"]

        # Capture = short the higher-funding venue, long the lower one.
        if f_hl >= f_ondo:
            short_leg, long_leg = "Hyperliquid", "Ondo"
            short_tag, long_tag = "xyz:" + canon, canon + "-USD.P"
        else:
            short_leg, long_leg = "Ondo", "Hyperliquid"
            short_tag, long_tag = canon + "-USD.P", "xyz:" + canon

        capture_hr = abs(f_hl - f_ondo)
        annual = capture_hr * HOURS_PER_YEAR

        ondo_taker = o.get("taker", 0.0005)
        round_trip_fee = 2.0 * (hl_taker + ondo_taker)   # open+close, both legs
        breakeven_hr = (round_trip_fee / capture_hr) if capture_hr > 0 else None

        # Thinner book caps how much size you can actually run delta-neutral.
        hl_oi = h.get("oi_usd")
        ondo_oi = o.get("oi_usd")
        oi_usd = None
        if hl_oi is not None and ondo_oi is not None:
            oi_usd = min(hl_oi, ondo_oi)

        # Basis = how far the two marks sit apart. A "delta-neutral" pair is only
        # neutral if the two oracles track; this is the gap you're exposed to.
        hl_mark = h.get("mark")
        ondo_mark = o.get("mark")
        basis_bps = None
        if hl_mark and ondo_mark:
            mid = (hl_mark + ondo_mark) / 2.0
            if mid:
                basis_bps = (ondo_mark - hl_mark) / mid * 1e4

        # Trailing-average funding (separate from the live instant rate). The
        # *_annual spreads are the durable run-rate vs the noisier instant one;
        # 7d smooths further than 24h.
        hl_avg = h.get("avg24_hr")
        ondo_avg = o.get("avg24_hr")
        avg24_capture = (abs(hl_avg - ondo_avg)
                         if (hl_avg is not None and ondo_avg is not None) else None)
        hl_avg7 = h.get("avg7d_hr")
        ondo_avg7 = o.get("avg7d_hr")
        avg7d_capture = (abs(hl_avg7 - ondo_avg7)
                         if (hl_avg7 is not None and ondo_avg7 is not None) else None)

        rows.append({
            "symbol": canon,
            "hl_funding_hr": f_hl,
            "ondo_funding_hr": f_ondo,
            "hl_funding_annual": f_hl * HOURS_PER_YEAR,
            "ondo_funding_annual": f_ondo * HOURS_PER_YEAR,
            "capture_hr": capture_hr,
            "annual": annual,
            "short_leg": short_leg,
            "long_leg": long_leg,
            "short_tag": short_tag,
            "long_tag": long_tag,
            "round_trip_fee": round_trip_fee,
            "breakeven_hr": breakeven_hr,
            "hl_oi_usd": hl_oi,
            "ondo_oi_usd": ondo_oi,
            "min_oi_usd": oi_usd,
            "hl_mark": hl_mark,
            "ondo_mark": ondo_mark,
            "basis_bps": basis_bps,
            "hl_samples": h.get("samples"),
            "ondo_samples": o.get("samples"),
            "funding_src": h.get("funding_src", "instant"),
            "hl_avg24_hr": hl_avg,
            "ondo_avg24_hr": ondo_avg,
            "hl_avg24_annual": (hl_avg * HOURS_PER_YEAR) if hl_avg is not None else None,
            "ondo_avg24_annual": (ondo_avg * HOURS_PER_YEAR) if ondo_avg is not None else None,
            "avg24_annual": (avg24_capture * HOURS_PER_YEAR) if avg24_capture is not None else None,
            "hl_avg24_n": h.get("avg24_n"),
            "ondo_avg24_n": o.get("avg24_n"),
            "hl_avg7d_hr": hl_avg7,
            "ondo_avg7d_hr": ondo_avg7,
            "hl_avg7d_annual": (hl_avg7 * HOURS_PER_YEAR) if hl_avg7 is not None else None,
            "ondo_avg7d_annual": (ondo_avg7 * HOURS_PER_YEAR) if ondo_avg7 is not None else None,
            "avg7d_annual": (avg7d_capture * HOURS_PER_YEAR) if avg7d_capture is not None else None,
            "hl_avg7d_n": h.get("avg7d_n"),
            "ondo_avg7d_n": o.get("avg7d_n"),
        })
    rows.sort(key=lambda r: r["annual"], reverse=True)
    return rows


def snapshot(dex=DEFAULT_DEX, hl_taker=DEFAULT_HL_TAKER, mock=False, avg_window=0,
             with_avg24=False):
    if mock:
        hl, ondo = _mock_payloads()
    else:
        hl = fetch_hyperliquid(dex)
        ondo = fetch_ondo()
    if avg_window and avg_window > 0:
        apply_funding_avg(hl, ondo, avg_window, mock=mock)
    if with_avg24:
        attach_funding_averages(hl, ondo, mock=mock)
    rows = compute_spreads(hl, ondo, hl_taker=hl_taker)
    basis = (f"{avg_window}h trailing avg" if avg_window and avg_window > 0
             else "instant (current interval)")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "venues": {"short_long_pair": "Hyperliquid(xyz) <-> Ondo Perps"},
        "funding_basis": basis,
        "avg_window": avg_window or 0,
        "avg24_window": AVG24_WINDOW_H if with_avg24 else 0,
        "avg7d_window": AVG7D_WINDOW_H if with_avg24 else 0,
        "hl_markets": len(hl),
        "ondo_markets": len(ondo),
        "paired_markets": len(rows),
        "rows": rows,
    }


def diagnose(dex=DEFAULT_DEX, mock=False):
    """Print raw OI/price fields from each venue so field mismatches are visible."""
    print("=== DIAGNOSE: raw venue fields (live) ===\n")
    if mock:
        print("(--mock set; diagnose only meaningful against live APIs)\n")
    # Hyperliquid
    try:
        payload = _http_json(HL_INFO_URL, method="POST",
                             body={"type": "metaAndAssetCtxs", "dex": dex})
        meta, ctxs = payload
        u0 = meta["universe"][0]
        c0 = ctxs[0]
        print(f"Hyperliquid first asset: {u0.get('name')}")
        print("  raw ctx keys:", sorted(c0.keys()))
        print("  openInterest:", c0.get("openInterest"),
              "| markPx:", c0.get("markPx"),
              "| oraclePx:", c0.get("oraclePx"))
        oi = _first_float(c0, ["openInterest"], 0.0)
        px = _first_float(c0, ["markPx", "midPx", "oraclePx"])
        print(f"  -> resolved oi_usd = {oi} x {px} = "
              f"{(oi*px) if px else None}\n")
    except Exception as exc:
        print(f"  Hyperliquid fetch failed: {exc}\n")
    # Ondo
    try:
        payload = _http_json(ONDO_CONTRACTS_URL, method="GET")
        result = payload.get("result", [])
        c0 = result[0] if result else {}
        print(f"Ondo first contract: {c0.get('market')}")
        print("  raw keys:", sorted(c0.keys()))
        print("  openInterest:", c0.get("openInterest"),
              "| openInterestUsd:", c0.get("openInterestUsd"),
              "| lastPrice:", c0.get("lastPrice"),
              "| indexPrice:", c0.get("indexPrice"))
        oiusd = _first_float(c0, ["openInterestUsd", "openInterestUSD", "oiUsd"])
        oibase = _first_float(c0, ["openInterest", "openInterestBase"])
        px = _first_float(c0, ["lastPrice", "indexPrice", "markPrice"])
        derived = oiusd if oiusd is not None else (
            oibase * px if (oibase is not None and px is not None) else None)
        print(f"  -> resolved oi_usd = {derived}  "
              f"(usd_field={oiusd}, base={oibase}, px={px})\n")
    except Exception as exc:
        print(f"  Ondo fetch failed: {exc}\n")
    # Resolved view through the normal pipeline
    try:
        snap = snapshot(dex=dex, mock=mock)
        print("Resolved per-row OI (first 6 paired):")
        for r in snap["rows"][:6]:
            print(f"  {r['symbol']:<6} HL={r['hl_oi_usd']}  Ondo={r['ondo_oi_usd']}")
    except Exception as exc:
        print(f"  pipeline failed: {exc}")


# ----------------------------------------------------------------------------
# Rendering (terminal)
# ----------------------------------------------------------------------------

def _pct(x, dp=2):
    return "n/a" if x is None else f"{x * 100:+.{dp}f}%"


def _annual_pct(x, dp=1):
    return "n/a" if x is None else f"{x * 100:.{dp}f}%"


def _hr_apr(hr):
    """'+0.0090% (79% yr)' — hourly funding with its annualised equivalent."""
    if hr is None:
        return "n/a"
    return f"{_pct(hr, 4)} ({_annual_pct(hr * HOURS_PER_YEAR, 0)} yr)"


def _hr_apr_c(hr):
    """Compact '+0.0090%(79%)' for wide multi-column tables."""
    if hr is None:
        return "n/a"
    return f"{_pct(hr, 4)}({_annual_pct(hr * HOURS_PER_YEAR, 0)})"


def _price(x):
    if x is None:
        return "n/a"
    return f"${x:,.0f}" if x >= 1000 else f"${x:.2f}"


def _money(x):
    if x is None:
        return "n/a"
    if x >= 1e6:
        return f"${x/1e6:.1f}M"
    if x >= 1e3:
        return f"${x/1e3:.0f}K"
    return f"${x:.0f}"


def render_table(snap, min_annual=0.0):
    rows = [r for r in snap["rows"] if r["annual"] >= min_annual]
    lines = []
    hdr = (f"Funding-rate arbitrage  |  Hyperliquid(xyz) <-> Ondo Perps  |  "
           f"{snap['generated_at']}")
    lines.append(hdr)
    lines.append(f"paired markets: {snap['paired_markets']}   "
                 f"(HL {snap['hl_markets']} / Ondo {snap['ondo_markets']})   "
                 f"shown >= {min_annual*100:.0f}% APR: {len(rows)}")
    lines.append(f"funding basis: {snap.get('funding_basis', 'instant')}")
    lines.append("-" * len(hdr))
    if not rows:
        lines.append("No overlapping markets above threshold.")
        return "\n".join(lines)

    colfmt = "{:<5} {:>9} {:>9} {:>7} {:>14} {:>14} {:>8} {:>7} {:>7} {:>20} {:>5}"
    lines.append(colfmt.format(
        "SYM", "HL px", "Ondo px", "BASIS",
        "HL fund (yr)", "Ondo fund (yr)", "SPRD APR",
        "HL OI", "Ondo OI", "DIRECTION", "B/E"))
    for r in rows:
        direction = f"short {_short(r['short_leg'])} / long {_short(r['long_leg'])}"
        be = "—" if r["breakeven_hr"] is None else f"{r['breakeven_hr']:.1f}"
        basis = "n/a" if r["basis_bps"] is None else f"{r['basis_bps']:+.0f}bp"
        lines.append(colfmt.format(
            r["symbol"],
            _price(r["hl_mark"]),
            _price(r["ondo_mark"]),
            basis,
            _hr_apr_c(r["hl_funding_hr"]),
            _hr_apr_c(r["ondo_funding_hr"]),
            _annual_pct(r["annual"]),
            _money(r["hl_oi_usd"]),
            _money(r["ondo_oi_usd"]),
            direction,
            be,
        ))
    lines.append("")
    lines.append("px = mark on each venue; BASIS = Ondo-vs-HL mark gap (your delta-neutral "
                 "exposure). fund hr (annual). SPRD APR = captured spread, annualised.")
    return "\n".join(lines)


def _short(leg):
    return "xyz" if leg == "Hyperliquid" else "Ondo"


# ----------------------------------------------------------------------------
# CSV
# ----------------------------------------------------------------------------

def append_csv(path, snap):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csvmod.writer(fh)
        if new:
            w.writerow(["ts", "symbol", "hl_mark", "ondo_mark", "basis_bps",
                        "hl_funding_hr", "ondo_funding_hr", "spread_hr", "annual",
                        "short_leg", "long_leg", "breakeven_hr",
                        "hl_oi_usd", "ondo_oi_usd"])
        for r in snap["rows"]:
            w.writerow([snap["generated_at"], r["symbol"],
                        "" if r["hl_mark"] is None else f"{r['hl_mark']:.4f}",
                        "" if r["ondo_mark"] is None else f"{r['ondo_mark']:.4f}",
                        "" if r["basis_bps"] is None else f"{r['basis_bps']:.1f}",
                        f"{r['hl_funding_hr']:.8f}", f"{r['ondo_funding_hr']:.8f}",
                        f"{r['capture_hr']:.8f}", f"{r['annual']:.6f}",
                        r["short_leg"], r["long_leg"],
                        "" if r["breakeven_hr"] is None else f"{r['breakeven_hr']:.2f}",
                        "" if r["hl_oi_usd"] is None else f"{r['hl_oi_usd']:.0f}",
                        "" if r["ondo_oi_usd"] is None else f"{r['ondo_oi_usd']:.0f}"])


# ----------------------------------------------------------------------------
# Dashboard server
# ----------------------------------------------------------------------------

def serve(port, host="127.0.0.1", dex=DEFAULT_DEX, hl_taker=DEFAULT_HL_TAKER,
          mock=False, cache_ttl=15, avg_window=0, with_avg24=True):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    here = os.path.dirname(os.path.abspath(__file__))
    dash_path = os.path.join(here, "fund_dashboard.html")

    # Single shared snapshot cache: protects the upstream APIs from being hit
    # once per viewer per refresh (Ondo rate-limits with 429). All clients read
    # the same cached snapshot until it goes stale.
    cache = {"at": 0.0, "data": None, "err": None}

    def cached_snapshot():
        now = time.time()
        if cache["data"] is not None and (now - cache["at"]) < cache_ttl:
            return cache["data"]
        try:
            cache["data"] = snapshot(dex=dex, hl_taker=hl_taker, mock=mock,
                                     avg_window=avg_window, with_avg24=with_avg24)
            cache["err"] = None
            cache["at"] = now
        except Exception as exc:
            cache["err"] = str(exc)
            if cache["data"] is None:
                raise
        return cache["data"]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/healthz":
                self._send(200, b'{"ok":true}', "application/json")
                return
            if path in ("/", "/index.html", "/dashboard.html"):
                try:
                    with open(dash_path, "rb") as fh:
                        self._send(200, fh.read(), "text/html; charset=utf-8")
                except FileNotFoundError:
                    self._send(500, b"dashboard.html not found next to funding_arb.py",
                               "text/plain")
                return
            if path == "/api/spreads":
                try:
                    snap = cached_snapshot()
                    self._send(200, json.dumps(snap).encode("utf-8"),
                               "application/json")
                except Exception as exc:  # surface upstream errors to the UI
                    self._send(502, json.dumps({"error": str(exc)}).encode("utf-8"),
                               "application/json")
                return
            self._send(404, b"not found", "text/plain")

    srv = ThreadingHTTPServer((host, port), Handler)
    shown = host if host != "0.0.0.0" else "0.0.0.0 (all interfaces)"
    print(f"Dashboard live at  http://{shown}:{port}"
          f"{'   (MOCK DATA)' if mock else ''}   cache={cache_ttl}s")
    print("Ctrl-C to stop.")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


# ----------------------------------------------------------------------------
# Mock data (offline self-test / demo)
# ----------------------------------------------------------------------------

def _mock_payloads():
    hl = {
        "NVDA":  {"funding_hr": 0.0000463, "avg24": 0.0000351, "avg7d": 0.0000300, "mark": 178.0, "oracle": 178.1, "oi_usd": 8_200_000, "raw_name": "xyz:NVDA"},
        "TSLA":  {"funding_hr": 0.0000125, "avg24": 0.0000210, "avg7d": 0.0000240, "mark": 412.0, "oracle": 412.2, "oi_usd": 6_100_000, "raw_name": "xyz:TSLA"},
        "AAPL":  {"funding_hr": -0.0000210, "avg24": -0.0000090, "avg7d": 0.0000020, "mark": 228.0, "oracle": 228.0, "oi_usd": 3_400_000, "raw_name": "xyz:AAPL"},
        "META":  {"funding_hr": 0.0000901, "avg24": 0.0000540, "avg7d": 0.0000470, "mark": 720.0, "oracle": 719.5, "oi_usd": 1_900_000, "raw_name": "xyz:META"},
        "AMZN":  {"funding_hr": 0.0000125, "avg24": 0.0000140, "avg7d": 0.0000165, "mark": 235.0, "oracle": 235.0, "oi_usd": 2_500_000, "raw_name": "xyz:AMZN"},
        "GOLD":  {"funding_hr": 0.0000332, "avg24": 0.0000300, "avg7d": 0.0000285, "mark": 3380.0, "oracle": 3380.0, "oi_usd": 14_000_000, "raw_name": "xyz:GOLD"},
        "XYZ100":{"funding_hr": 0.0000200, "avg24": 0.0000180, "avg7d": 0.0000175, "mark": 26100.0, "oracle": 26090.0, "oi_usd": 70_000_000, "raw_name": "xyz:XYZ100"},
    }
    ondo = {
        "NVDA": {"funding_hr": 0.0000125, "avg24": 0.0000150, "avg7d": 0.0000142, "funding_last": 0.0000130, "mark": 178.1, "oi_usd": 1_200_000, "taker": 0.0005, "maker": 0.0002, "raw_name": "NVDA-USD.P"},
        "TSLA": {"funding_hr": 0.0000540, "avg24": 0.0000420, "avg7d": 0.0000380, "funding_last": 0.0000500, "mark": 412.3, "oi_usd": 900_000, "taker": 0.0005, "maker": 0.0002, "raw_name": "TSLA-USD.P"},
        "AAPL": {"funding_hr": 0.0000125, "avg24": 0.0000120, "avg7d": 0.0000125, "funding_last": 0.0000125, "mark": 228.1, "oi_usd": 700_000, "taker": 0.0005, "maker": 0.0002, "raw_name": "AAPL-USD.P"},
        "META": {"funding_hr": 0.0000180, "avg24": 0.0000160, "avg7d": 0.0000155, "funding_last": 0.0000200, "mark": 719.0, "oi_usd": 500_000, "taker": 0.0005, "maker": 0.0002, "raw_name": "META-USD.P"},
        "AMZN": {"funding_hr": 0.0000125, "avg24": 0.0000130, "avg7d": 0.0000135, "funding_last": 0.0000125, "mark": 235.1, "oi_usd": 600_000, "taker": 0.0005, "maker": 0.0002, "raw_name": "AMZN-USD.P"},
        "XAU":  {"funding_hr": 0.0000125, "avg24": 0.0000125, "avg7d": 0.0000128, "funding_last": 0.0000125, "mark": 3381.0, "oi_usd": 2_000_000, "taker": 0.0004, "maker": 0.0001, "raw_name": "XAU-USD.P"},
        "QQQ":  {"funding_hr": 0.0000150, "avg24": 0.0000150, "avg7d": 0.0000150, "funding_last": 0.0000150, "mark": 540.0, "oi_usd": 3_000_000, "taker": 0.0005, "maker": 0.0002, "raw_name": "QQQ-USD.P"},
    }
    return hl, ondo


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def main(argv=None):
    p = argparse.ArgumentParser(description="Funding-rate arb: Hyperliquid xyz <-> Ondo Perps")
    p.add_argument("--watch", type=int, metavar="SECONDS",
                   help="refresh continuously every SECONDS")
    p.add_argument("--min-annual", type=float, default=0.0, metavar="PCT",
                   help="hide spreads below PCT annualised (e.g. 20 for 20%%)")
    p.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    p.add_argument("--csv", metavar="PATH", help="append snapshot rows to a CSV file")
    p.add_argument("--serve", nargs="?", type=int, const=-1, metavar="PORT",
                   help="serve the live dashboard (PORT defaults to $PORT or 8787)")
    p.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"),
                   help="bind host (use 0.0.0.0 to deploy; default 127.0.0.1)")
    p.add_argument("--cache-ttl", type=int, default=15, metavar="SECONDS",
                   help="server-side snapshot cache TTL (default 15s)")
    p.add_argument("--dex", default=DEFAULT_DEX, help="Hyperliquid HIP-3 dex name (default: xyz)")
    p.add_argument("--hl-taker", type=float, default=DEFAULT_HL_TAKER,
                   help="Hyperliquid taker fee fraction per leg (default: 0.00045)")
    p.add_argument("--avg-window", type=int, default=0, metavar="HOURS",
                   help="use trailing HOURS average funding instead of the current "
                        "interval (e.g. 24). Costs 2 extra calls per market; pair "
                        "with a longer --cache-ttl / --watch.")
    p.add_argument("--diagnose", action="store_true",
                   help="print raw OI/price fields from each venue and exit (debug)")
    p.add_argument("--mock", action="store_true", help="use bundled sample data (offline)")
    args = p.parse_args(argv)

    min_annual = args.min_annual / 100.0

    if args.diagnose:
        diagnose(dex=args.dex, mock=args.mock)
        return 0

    if args.serve is not None:
        port = args.serve if (args.serve and args.serve > 0) \
            else int(os.environ.get("PORT", 8787))
        serve(port, host=args.host, dex=args.dex, hl_taker=args.hl_taker,
              mock=args.mock, cache_ttl=args.cache_ttl, avg_window=args.avg_window)
        return 0

    def run_once():
        snap = snapshot(dex=args.dex, hl_taker=args.hl_taker, mock=args.mock,
                        avg_window=args.avg_window)
        if args.csv:
            append_csv(args.csv, snap)
        if args.json:
            print(json.dumps(snap, indent=2))
        else:
            print(render_table(snap, min_annual=min_annual))
        return snap

    if args.watch:
        try:
            while True:
                if not args.json:
                    os.system("clear" if os.name != "nt" else "cls")
                try:
                    run_once()
                except (urllib.error.URLError, ValueError, TimeoutError) as exc:
                    print(f"[fetch error] {exc}", file=sys.stderr)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            return 0
    else:
        try:
            run_once()
        except (urllib.error.URLError, ValueError, TimeoutError) as exc:
            print(f"[fetch error] {exc}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
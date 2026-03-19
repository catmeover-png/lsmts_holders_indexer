import os
import json
import time
import random
import itertools
from datetime import datetime
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
MORALIS_API_KEY = os.environ["MORALIS_API_KEY"]
BASE_RPC_URL = os.environ["BASE_RPC_URL"]          # Alchemy (primary)
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

# Дополнительные публичные RPC для Base (без ключа)
FALLBACK_RPC_URLS = [
    "https://mainnet.base.org",
    "https://base.llamarpc.com",
]

TOKEN_ADDRESS = "0x9EadbE35F3Ee3bF3e28180070C429298a1b02F93"
CHAIN = "base"
TOKEN_DECIMALS = 18

MORALIS_BASE_URL = "https://deep-index.moralis.io/api/v2.2"
PAGE_LIMIT = 100
MAX_PAGES = 350

MIN_BALANCE = 0.001
MIN_CHANGE_PERCENT = 5

REQUEST_SLEEP_SEC = 0.05

# RPC — фиксированный batch size, без динамического роста
RPC_BATCH_SIZE = 40           # фиксированный, не растёт
RPC_BATCH_SIZE_MIN = 15       # минимум при 429
RPC_SLEEP_SEC = 0.05
RPC_MAX_RETRIES_429 = 6
RPC_RETRY_BASE_SEC = 1.2
RPC_RETRY_NON429 = 3
RPC_MAX_WORKERS = 2           # 2 воркера — по одному на каждый RPC

# Второй проход для rpc_error
RPC_RETRY_PASS_BATCH = 20     # меньший батч для ретрая

MORALIS_MAX_RETRIES_429 = 5
MORALIS_RETRY_BASE_SEC = 3.0

SHEET_HOLDERS = "holders"
SHEET_LABELS = "labels"
SHEET_HISTORY = "history"
SHEET_MOVEMENTS = "movements"
SHEET_HOLDERS_RAW = "holders_raw"


# =========================
# RPC ROUND-ROBIN
# =========================
_all_rpc_urls = [BASE_RPC_URL] + FALLBACK_RPC_URLS
_rpc_cycle = itertools.cycle(_all_rpc_urls)
_rpc_cycle_lock = threading.Lock()

def next_rpc_url() -> str:
    with _rpc_cycle_lock:
        return next(_rpc_cycle)

# Per-RPC динамический batch size (независимо для каждого endpoint)
_batch_sizes: Dict[str, int] = {url: RPC_BATCH_SIZE for url in _all_rpc_urls}
_batch_size_lock = threading.Lock()

def get_batch_size(rpc_url: str) -> int:
    with _batch_size_lock:
        return _batch_sizes.get(rpc_url, RPC_BATCH_SIZE)

def decrease_batch_size(rpc_url: str):
    with _batch_size_lock:
        cur = _batch_sizes.get(rpc_url, RPC_BATCH_SIZE)
        new = max(RPC_BATCH_SIZE_MIN, cur // 2)
        _batch_sizes[rpc_url] = new
        print(f"  [RPC] {rpc_url[-30:]} batch ↓ {cur}→{new}")

def reset_batch_size(rpc_url: str):
    """После успеха возвращаем к базовому, но не растём выше него."""
    with _batch_size_lock:
        cur = _batch_sizes.get(rpc_url, RPC_BATCH_SIZE)
        if cur < RPC_BATCH_SIZE:
            _batch_sizes[rpc_url] = min(RPC_BATCH_SIZE, cur + 5)


# =========================
# HELPERS
# =========================
def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def n0(x) -> float:
    try:
        n = float(x)
        return n if n == n else 0.0
    except Exception:
        return 0.0

def to_tokens(raw: str) -> float:
    return int(raw) / (10 ** TOKEN_DECIMALS)

def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def jitter_sleep(base_sec: float, attempt: int):
    """Экспоненциальный backoff с jitter чтобы ретраи не синхронизировались."""
    wait = base_sec * attempt * random.uniform(0.7, 1.4)
    time.sleep(wait)


# =========================
# GOOGLE SHEETS
# =========================
def get_gspread_client():
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def get_spreadsheet():
    gc = get_gspread_client()
    return gc.open_by_key(SPREADSHEET_ID)

def get_or_create_worksheet(spreadsheet, title: str, rows: int = 1000, cols: int = 20):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)

def clear_and_write(ws, values: List[List]):
    ws.clear()
    if values:
        ws.update(values)


# =========================
# READ EXISTING SHEETS
# =========================
def read_labels(spreadsheet) -> Dict[str, str]:
    ws = get_or_create_worksheet(spreadsheet, SHEET_LABELS)
    values = ws.get_all_values()
    if len(values) <= 1:
        return {}
    out = {}
    for row in values[1:]:
        if not row:
            continue
        addr = (row[0] if len(row) > 0 else "").strip().lower()
        label = (row[1] if len(row) > 1 else "").strip()
        if addr:
            out[addr] = label
    return out

def read_previous_holders(spreadsheet) -> Dict[str, float]:
    ws = get_or_create_worksheet(spreadsheet, SHEET_HOLDERS)
    values = ws.get_all_values()
    if len(values) <= 1:
        return {}
    out = {}
    for row in values[1:]:
        if not row:
            continue
        addr = (row[0] if len(row) > 0 else "").strip().lower()
        bal = n0(row[1] if len(row) > 1 else 0)
        if addr:
            out[addr] = bal
    return out


# =========================
# MORALIS
# =========================
def moralis_get_json(url: str, attempt: int = 1) -> dict:
    headers = {"X-API-Key": MORALIS_API_KEY}
    resp = requests.get(url, headers=headers, timeout=60)

    if resp.status_code == 429:
        if attempt >= MORALIS_MAX_RETRIES_429:
            raise RuntimeError(f"Moralis 429 after retries: {resp.text[:300]}")
        jitter_sleep(MORALIS_RETRY_BASE_SEC, attempt)
        return moralis_get_json(url, attempt + 1)

    if resp.status_code in (500, 502, 503, 504):
        if attempt >= MORALIS_MAX_RETRIES_429:
            raise RuntimeError(f"Moralis {resp.status_code} after retries: {resp.text[:300]}")
        jitter_sleep(MORALIS_RETRY_BASE_SEC, attempt)
        return moralis_get_json(url, attempt + 1)

    resp.raise_for_status()
    return resp.json()

def fetch_all_owners() -> List[Dict]:
    out = []
    cursor = None
    seen_cursors = set()
    seen_signatures = set()

    for page_num in range(MAX_PAGES):
        url = (
            f"{MORALIS_BASE_URL}/erc20/{TOKEN_ADDRESS}/owners"
            f"?chain={CHAIN}&limit={PAGE_LIMIT}&order=DESC"
        )
        if cursor:
            url += f"&cursor={cursor}"

        data = moralis_get_json(url)
        arr = data.get("result", [])
        next_cursor = data.get("cursor")

        if not arr:
            break

        first_addr = (arr[0].get("owner_address") or arr[0].get("address") or "").lower()
        last_addr = (arr[-1].get("owner_address") or arr[-1].get("address") or "").lower()
        signature = f"{len(arr)}|{first_addr}|{last_addr}"

        if signature in seen_signatures:
            break
        seen_signatures.add(signature)

        for r in arr:
            address = (r.get("owner_address") or r.get("address") or "").strip().lower()
            balance_raw = r.get("balance")
            if address and balance_raw:
                bal = to_tokens(balance_raw)
                if bal > 0:
                    out.append({"address": address, "moralis_balance": bal})

        if not next_cursor or next_cursor in seen_cursors:
            break

        seen_cursors.add(next_cursor)
        cursor = next_cursor

        if page_num % 5 == 4:
            time.sleep(REQUEST_SLEEP_SEC)

    dedup = {}
    for row in out:
        dedup[row["address"]] = row["moralis_balance"]

    final = [{"address": a, "moralis_balance": b} for a, b in dedup.items()]
    final.sort(key=lambda x: x["moralis_balance"], reverse=True)
    return final


# =========================
# RPC
# =========================
def pad64(addr: str) -> str:
    return addr.lower().replace("0x", "").rjust(64, "0")

def is_429_like(resp: requests.Response, body_text: str, json_error=None) -> bool:
    if resp.status_code == 429:
        return True
    if "compute units per second" in body_text or "throughput" in body_text.lower():
        return True
    if json_error:
        err_text = json.dumps(json_error)
        if str(json_error.get("code")) == "429":
            return True
        if "compute units per second" in err_text or "throughput" in err_text.lower():
            return True
    return False

def make_balance_call(address: str, req_id: int) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "eth_call",
        "params": [
            {"to": TOKEN_ADDRESS, "data": "0x70a08231" + pad64(address)},
            "latest",
        ],
    }

def rpc_balance_of_batch(
    addresses: List[str],
    rpc_url: str,
    attempt_429: int = 1,
    attempt_non429: int = 1,
) -> Dict[str, Tuple[str, float]]:
    id_to_address = {}
    payload = []

    for idx, address in enumerate(addresses, start=1):
        id_to_address[idx] = address
        payload.append(make_balance_call(address, idx))

    try:
        resp = requests.post(rpc_url, json=payload, timeout=60)
        body = resp.text

        if is_429_like(resp, body):
            decrease_batch_size(rpc_url)
            if attempt_429 >= RPC_MAX_RETRIES_429:
                raise RuntimeError(f"RPC batch 429 after retries ({rpc_url[-30:]}): {body[:200]}")
            jitter_sleep(RPC_RETRY_BASE_SEC, attempt_429)
            return rpc_balance_of_batch(addresses, rpc_url, attempt_429 + 1, attempt_non429)

        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list):
            if attempt_non429 <= RPC_RETRY_NON429:
                jitter_sleep(1.0, attempt_non429)
                return rpc_balance_of_batch(addresses, rpc_url, attempt_429, attempt_non429 + 1)
            raise RuntimeError(f"Unexpected batch response from {rpc_url[-30:]}: {str(data)[:200]}")

        reset_batch_size(rpc_url)

        out = {address: ("rpc_error", 0.0) for address in addresses}

        for item in data:
            req_id = item.get("id")
            address = id_to_address.get(req_id)
            if not address:
                continue

            if item.get("error"):
                err = item["error"]
                if is_429_like(resp, json.dumps(err), err):
                    decrease_batch_size(rpc_url)
                    if attempt_429 >= RPC_MAX_RETRIES_429:
                        raise RuntimeError(f"RPC logical 429 after retries")
                    jitter_sleep(RPC_RETRY_BASE_SEC, attempt_429)
                    return rpc_balance_of_batch(addresses, rpc_url, attempt_429 + 1, attempt_non429)
                out[address] = ("rpc_error", 0.0)
                continue

            raw_hex = item.get("result", "0x0")
            raw_int = int(raw_hex, 16)
            bal = raw_int / (10 ** TOKEN_DECIMALS)
            out[address] = ("zero", 0.0) if bal <= 0 else ("verified", bal)

        return out

    except Exception as e:
        msg = str(e)
        if "429" in msg or "compute units per second" in msg or "throughput" in msg.lower():
            decrease_batch_size(rpc_url)
            if attempt_429 >= RPC_MAX_RETRIES_429:
                raise
            jitter_sleep(RPC_RETRY_BASE_SEC, attempt_429)
            return rpc_balance_of_batch(addresses, rpc_url, attempt_429 + 1, attempt_non429)

        if attempt_non429 <= RPC_RETRY_NON429:
            jitter_sleep(1.0, attempt_non429)
            return rpc_balance_of_batch(addresses, rpc_url, attempt_429, attempt_non429 + 1)
        raise


# =========================
# VERIFY — round-robin + второй проход
# =========================
def run_rpc_pass(
    addresses: List[str],
    batch_size: int,
    label: str = "pass",
) -> Dict[str, Tuple[str, float]]:
    """
    Один проход по списку адресов: нарезает на батчи,
    раздаёт их воркерам, каждый батч идёт на следующий RPC из цикла.
    """
    results: Dict[str, Tuple[str, float]] = {}
    lock = threading.Lock()

    batches = list(chunks(addresses, batch_size))
    print(f"  [{label}] {len(addresses)} addrs → {len(batches)} batches (size={batch_size})")

    def process_batch(batch_addresses: List[str], rpc_url: str):
        batch_result = rpc_balance_of_batch(batch_addresses, rpc_url)
        time.sleep(RPC_SLEEP_SEC)
        with lock:
            results.update(batch_result)

    with ThreadPoolExecutor(max_workers=RPC_MAX_WORKERS) as executor:
        futures = {}
        for batch in batches:
            rpc_url = next_rpc_url()   # round-robin
            f = executor.submit(process_batch, batch, rpc_url)
            futures[f] = rpc_url

        done = 0
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"  [warn] batch error: {e}")
            done += 1
            if done % 20 == 0 or done == len(futures):
                print(f"  [{label}] {done}/{len(futures)} batches done")

    return results

def verify_all(rows: List[Dict]) -> List[Dict]:
    skip_rpc = [r for r in rows if r["moralis_balance"] < MIN_BALANCE]
    need_rpc  = [r for r in rows if r["moralis_balance"] >= MIN_BALANCE]

    print(f"  [verify] total={len(rows)}, need_rpc={len(need_rpc)}, skip_rpc={len(skip_rpc)}")

    # Первый проход
    addresses = [r["address"] for r in need_rpc]
    results = run_rpc_pass(addresses, RPC_BATCH_SIZE, label="pass1")

    # Второй проход — только rpc_error адреса, меньший батч
    error_addrs = [a for a, (status, _) in results.items() if status == "rpc_error"]
    if error_addrs:
        print(f"  [pass2] retrying {len(error_addrs)} rpc_error addresses...")
        results2 = run_rpc_pass(error_addrs, RPC_RETRY_PASS_BATCH, label="pass2")
        results.update(results2)  # перезаписываем ошибки улучшенными результатами

    # Собираем финальный список
    verified = []
    for r in need_rpc:
        address = r["address"]
        status, live_balance = results.get(address, ("rpc_error", 0.0))
        verified.append({
            "address": address,
            "moralis_balance": r["moralis_balance"],
            "verified_balance": live_balance if status != "rpc_error" else "",
            "verify_status": status,
        })

    for r in skip_rpc:
        verified.append({
            "address": r["address"],
            "moralis_balance": r["moralis_balance"],
            "verified_balance": r["moralis_balance"],
            "verify_status": "skipped",
        })

    return verified


# =========================
# MOVEMENTS / HISTORY
# =========================
def analyze_movements(
    current_holders: List[Dict],
    previous_balances: Dict[str, float],
    labels: Dict[str, str],
) -> List[Dict]:
    movements = []
    current_map = {h["address"]: h["balance"] for h in current_holders}
    current_addresses = set(current_map.keys())

    for h in current_holders:
        addr = h["address"]
        now_bal = h["balance"]
        prev_bal = previous_balances.get(addr, 0)

        if prev_bal > 0:
            change = now_bal - prev_bal
            change_pct = (change / prev_bal) * 100
            if abs(change_pct) >= MIN_CHANGE_PERCENT:
                movements.append({
                    "address": addr,
                    "label": labels.get(addr, "Retail"),
                    "was": prev_bal,
                    "now": now_bal,
                    "change": change,
                    "change_percent": change_pct,
                    "action": "🟢 КУПИЛ" if change > 0 else "🔴 ПРОДАЛ",
                })
        else:
            if now_bal >= MIN_BALANCE:
                movements.append({
                    "address": addr,
                    "label": labels.get(addr, "Retail"),
                    "was": 0,
                    "now": now_bal,
                    "change": now_bal,
                    "change_percent": 100,
                    "action": "🆕 НОВЫЙ",
                })

    for addr, prev_bal in previous_balances.items():
        if addr not in current_addresses:
            movements.append({
                "address": addr,
                "label": labels.get(addr, "Retail"),
                "was": prev_bal,
                "now": 0,
                "change": -prev_bal,
                "change_percent": -100,
                "action": "🔻 ВЫШЕЛ",
            })

    movements.sort(key=lambda x: x["change"])
    return movements


# =========================
# SHEETS WRITE
# =========================
def write_holders_raw(spreadsheet, verified_rows: List[Dict], labels: Dict[str, str]):
    ws = get_or_create_worksheet(
        spreadsheet, SHEET_HOLDERS_RAW,
        rows=max(1000, len(verified_rows) + 20), cols=10
    )
    values = [["wallet", "moralis_balance", "verified_balance", "verify_status", "label"]]
    for r in verified_rows:
        values.append([
            r["address"],
            round(float(r["moralis_balance"]), 6),
            "" if r["verified_balance"] == "" else round(float(r["verified_balance"]), 6),
            r["verify_status"],
            labels.get(r["address"], "Retail"),
        ])
    clear_and_write(ws, values)

def build_clean_holders(verified_rows: List[Dict]) -> List[Dict]:
    out = []
    for r in verified_rows:
        if r["verify_status"] in ("verified", "skipped"):
            bal = float(r["verified_balance"])
            if bal >= MIN_BALANCE:
                out.append({"address": r["address"], "balance": bal})
    out.sort(key=lambda x: x["balance"], reverse=True)
    return out

def write_holders(spreadsheet, holders: List[Dict], labels: Dict[str, str]):
    ws = get_or_create_worksheet(
        spreadsheet, SHEET_HOLDERS,
        rows=max(1000, len(holders) + 20), cols=10
    )
    total = sum(h["balance"] for h in holders)
    values = [["Wallet", "Balance", "Label", "% of total"]]
    for h in holders:
        pct = (h["balance"] / total * 100) if total > 0 else 0
        values.append([
            h["address"],
            round(h["balance"], 6),
            labels.get(h["address"], "Retail"),
            f"{pct:.4f}%",
        ])
    clear_and_write(ws, values)

def append_movements(spreadsheet, movements: List[Dict], timestamp: str):
    ws = get_or_create_worksheet(spreadsheet, SHEET_MOVEMENTS, rows=1000, cols=10)
    existing = ws.get_all_values()
    if not existing:
        ws.update([["Timestamp", "Action", "Wallet", "Label", "Was", "Now", "Change", "Change %"]])
    if not movements:
        return
    rows = []
    for m in movements:
        rows.append([
            timestamp, m["action"], m["address"], m["label"],
            round(m["was"], 6), round(m["now"], 6),
            round(m["change"], 6), f"{m['change_percent']:.2f}%",
        ])
    ws.append_rows(rows, value_input_option="RAW")

def append_history(spreadsheet, holders: List[Dict], labels: Dict[str, str], timestamp: str):
    ws = get_or_create_worksheet(spreadsheet, SHEET_HISTORY, rows=1000, cols=10)
    existing = ws.get_all_values()
    if not existing:
        ws.update([["timestamp", "label", "balance", "total", "retail_%"]])

    total = sum(h["balance"] for h in holders)
    label_totals = {}
    for h in holders:
        label = labels.get(h["address"], "Retail")
        label_totals[label] = label_totals.get(label, 0) + h["balance"]

    retail = label_totals.get("Retail", 0)
    retail_pct = (retail / total * 100) if total > 0 else 0

    rows = []
    for label in sorted(label_totals.keys(), key=lambda x: (x != "Retail", x)):
        rows.append([
            timestamp, label,
            round(label_totals[label], 6),
            round(total, 6),
            round(retail_pct, 4),
        ])
    if rows:
        ws.append_rows(rows, value_input_option="RAW")


# =========================
# MAIN
# =========================
def main():
    t0 = time.time()
    spreadsheet = get_spreadsheet()

    labels = read_labels(spreadsheet)
    previous_balances = read_previous_holders(spreadsheet)

    print("Fetching owners from Moralis...")
    raw_rows = fetch_all_owners()
    print(f"  Got {len(raw_rows)} owners in {time.time()-t0:.1f}s")

    t1 = time.time()
    print("Verifying via RPC...")
    verified_rows = verify_all(raw_rows)
    print(f"  Verified in {time.time()-t1:.1f}s")

    print("Writing sheets...")
    t2 = time.time()
    write_holders_raw(spreadsheet, verified_rows, labels)
    clean_holders = build_clean_holders(verified_rows)
    write_holders(spreadsheet, clean_holders, labels)

    timestamp = now_stamp()
    movements = analyze_movements(clean_holders, previous_balances, labels)
    append_movements(spreadsheet, movements, timestamp)
    append_history(spreadsheet, clean_holders, labels, timestamp)
    print(f"  Sheets written in {time.time()-t2:.1f}s")

    verified_ok = sum(1 for r in verified_rows if r["verify_status"] == "verified")
    zeros      = sum(1 for r in verified_rows if r["verify_status"] == "zero")
    errors     = sum(1 for r in verified_rows if r["verify_status"] == "rpc_error")
    skipped    = sum(1 for r in verified_rows if r["verify_status"] == "skipped")

    print(
        f"Done in {time.time()-t0:.1f}s. "
        f"raw={len(raw_rows)}, verified={verified_ok}, "
        f"zero={zeros}, rpc_error={errors}, skipped={skipped}, clean={len(clean_holders)}"
    )


if __name__ == "__main__":
    main()

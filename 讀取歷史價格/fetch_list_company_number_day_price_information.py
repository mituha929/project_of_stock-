import os
import pandas as pd
import requests
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm

# ---------- 設定 ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, os.pardir))
input_csv = os.path.join(PROJECT_ROOT, "data", "list_company_number.csv")
output_dir = os.path.join(PROJECT_ROOT, "data", "daily_stock_data")
os.makedirs(output_dir, exist_ok=True)

URL_TEMPLATE = "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={yyyymm}01&stockNo={stock_id}"
LOOKBACK_MONTHS = 6
MAX_RETRIES = 3
BACKOFF_FACTOR = 1.5
REQUEST_TIMEOUT = 10
GLOBAL_RATE_PER_MINUTE = 50  # 全域速率限制

# ---------- Header 池（隨機化） ----------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
]
ACCEPT_LANGS = [
    "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "zh-Hant-TW,zh-Hant;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,zh-TW;q=0.8"
]

def make_headers():
    """隨機生成正常範圍內的 header"""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.twse.com.tw/",
        "Accept-Language": random.choice(ACCEPT_LANGS)
    }

# ---------- Rate Limiter ----------
class RateLimiter:
    def __init__(self, per_minute):
        self.capacity = per_minute
        self.tokens = per_minute
        self.lock = Lock()
        self.last_refill = time.time()

    def acquire(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            refill = elapsed * (self.capacity / 60)
            if refill > 0:
                self.tokens = min(self.capacity, self.tokens + refill)
                self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    def wait(self):
        while not self.acquire():
            time.sleep(0.2)

# ---------- 股票代號讀取 ----------
def load_stock_ids():
    if os.path.exists(input_csv):
        df = pd.read_csv(input_csv)
        return df.iloc[:, 0].astype(str).tolist()
    return []

# ---------- 工具函式 ----------
def decrement_month(dt: datetime) -> datetime:
    year, month = dt.year, dt.month - 1
    if month == 0:
        year -= 1
        month = 12
    return dt.replace(year=year, month=month, day=1)

def gen_target_months(n, ref_date=None):
    if ref_date is None:
        ref_date = datetime.today()
    current = ref_date.replace(day=1)
    return [(current.year - (i // 12), ((current.month - i - 1) % 12) + 1) for i in range(n)]

def already_has_month(csv_path, year, month):
    """檢查是否已有該月份資料（智慧快取）"""
    if not os.path.exists(csv_path):
        return False
    try:
        df_existing = pd.read_csv(csv_path, dtype=str)
        if "日期" not in df_existing.columns:
            return False
        prefix = f"{year:04d}-{month:02d}-"
        return df_existing["日期"].astype(str).str.startswith(prefix).any()
    except:
        return False

# ---------- 單支股票抓取 ----------
def fetch_stock_price_data(stock_id, rate_limiter):
    session = requests.Session()
    months = gen_target_months(LOOKBACK_MONTHS)
    all_data = []
    out_path = os.path.join(output_dir, f"{stock_id}.csv")

    for year, month in tqdm(months, desc=f"{stock_id} 月份", leave=False, unit="月"):
        if already_has_month(out_path, year, month):
            continue  # 跳過已有資料

        yyyymm = f"{year:04d}{month:02d}"
        url = URL_TEMPLATE.format(yyyymm=yyyymm, stock_id=stock_id)
        success = False

        for attempt in range(MAX_RETRIES):
            rate_limiter.wait()
            try:
                resp = session.get(url, headers=make_headers(), timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                json_data = resp.json()
                if json_data.get("data"):
                    for row in json_data["data"]:
                        parts = row[0].split('/')
                        if len(parts) != 3:
                            continue
                        roc_year = int(parts[0])
                        date_str = f"{roc_year + 1911:04d}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                        close = row[6].replace(',', '').strip()
                        all_data.append((date_str, close))
                success = True
                break
            except:
                time.sleep(BACKOFF_FACTOR ** (attempt + 1))

        time.sleep(random.uniform(1, 3))  # 避免太快被鎖

    # 儲存資料（合併舊資料）
    if all_data:
        df_out = pd.DataFrame(all_data, columns=["日期", "收盤價"]).drop_duplicates()
        if os.path.exists(out_path):
            old_df = pd.read_csv(out_path, dtype=str)
            df_out = pd.concat([old_df, df_out]).drop_duplicates()
        df_out.sort_values("日期", ascending=False).to_csv(out_path, index=False, encoding="utf-8-sig")

# ---------- 主程式 ----------
def main():
    stock_id_list = load_stock_ids()
    rate_limiter = RateLimiter(GLOBAL_RATE_PER_MINUTE)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(fetch_stock_price_data, sid, rate_limiter): sid for sid in stock_id_list}
        for _ in tqdm(as_completed(futures), total=len(futures), desc="股票", unit="支"):
            pass

if __name__ == "__main__":
    main()

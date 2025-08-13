import requests
import pandas as pd
import os
import time
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# === 路徑設定 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, os.pardir))
SAVE_DIR = os.path.join(PROJECT_ROOT, "data", "list_company_stock_data")
os.makedirs(SAVE_DIR, exist_ok=True)

# === 常見 User-Agent 清單 ===
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
]

# === 讀取股票代號 ===
def read_stock_codes():
    csv_path = os.path.join(PROJECT_ROOT, "data", "list_company_number.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到股票代號檔案：{csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    return df.iloc[:, 0].dropna().astype(str).tolist()

# === 隨機 header ===
def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://www.twse.com.tw/zh/trading/historical/stock-day.html",
        "X-Requested-With": "XMLHttpRequest"
    }

# === 請求封裝，帶 retry ===
def safe_get(url, headers, retries=3, delay=2):
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            return res
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                raise e

# === 抓取單一股票半年資料 ===
def fetch_twse_stock(code: str):
    time.sleep(random.uniform(1.0, 2.0))  # 啟動前延遲

    output_path = os.path.join(SAVE_DIR, f"{code}.csv")
    existing_df = pd.read_csv(output_path, encoding="utf-8-sig") if os.path.exists(output_path) else pd.DataFrame()

    all_rows = []
    today = datetime.now()

    # 往前抓取 6 個月
    for i in range(6):
        target_date = today - relativedelta(months=i)
        date_str = target_date.strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={date_str}&stockNo={code}&response=json"

        try:
            res = safe_get(url, get_random_headers())
            json_data = res.json()

            if json_data.get("stat") != "OK":
                continue

            data = json_data.get("data", [])
            for item in data:
                date_val = item[0]  # 日期
                volume = int(item[1].replace(",", ""))  # 成交股數
                amount = int(item[2].replace(",", ""))  # 成交金額
                high = float(item[4].replace(",", ""))  # 最高價
                low = float(item[5].replace(",", ""))   # 最低價
                avg_price = round(amount / volume, 2) if volume else 0
                trades = int(item[8].replace(",", ""))  # 成交筆數

                all_rows.append([date_val, volume, amount, high, low, avg_price, trades])

        except Exception as e:
            print(f"⚠️ [{code}] 抓取 {target_date.strftime('%Y-%m')} 失敗: {e}")

        time.sleep(random.uniform(1.0, 1.5))  # 每月請求間延遲

    # 建立 DataFrame
    if all_rows:
        df = pd.DataFrame(all_rows, columns=["日期", "成交股數", "成交金額", "成交最高", "成交最低", "成交均價", "成交筆數"])

        # 合併舊資料並去重
        if not existing_df.empty:
            df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates(subset=["日期"], keep="last")

        # 排序
        df.sort_values(by="日期", ascending=False, inplace=True, ignore_index=True)

        # 儲存
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ [{code}] 半年資料已更新，共 {len(df)} 筆")
    else:
        print(f"⚠️ [{code}] 半年內無資料")

# === 包裝函式給執行緒使用 ===
def fetch_task(code):
    fetch_twse_stock(code)

# === 主程式 ===
if __name__ == "__main__":
    try:
        stock_codes = read_stock_codes()
    except Exception as e:
        print(f"讀取股票代號失敗：{e}")
        exit(1)

    MAX_WORKERS = 2
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_task, code): code for code in stock_codes}
        with tqdm(total=len(stock_codes), desc="上市股票半年資料進度") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ [{futures[future]}] 執行失敗：{e}")
                pbar.update(1)

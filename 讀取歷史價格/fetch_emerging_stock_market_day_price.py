import requests
import pandas as pd
import os
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# === 路徑設定 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, os.pardir))
SAVE_DIR = os.path.join(PROJECT_ROOT, "data", "emerging_stock_data")
os.makedirs(SAVE_DIR, exist_ok=True)

# === 常見 User-Agent 清單 ===
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
]

# === 讀取興櫃股票代號 ===
def read_stock_codes():
    csv_path = os.path.join(PROJECT_ROOT, "data", "emerging_stock_market.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"找不到股票代號檔案：{csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    return df.iloc[:, 0].dropna().astype(str).tolist()

# === 隨機 header ===
def get_random_headers():
    return {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://www.tpex.org.tw/zh-tw/emerging/historical",
        "User-Agent": random.choice(USER_AGENTS),
        "X-Requested-With": "XMLHttpRequest",
    }

# === 請求封裝，帶 retry ===
def safe_post(url, headers, data, retries=3, delay=2):
    for attempt in range(retries):
        try:
            res = requests.post(url, headers=headers, data=data, timeout=10)
            res.raise_for_status()
            return res
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                raise e

# === 抓取單一興櫃股票資料 ===
def fetch_emerging_stock(code: str, start_roc_year: int, start_month: int, months: int = 12):
    time.sleep(random.uniform(1.0, 2.0))  # 降低被鎖機率

    output_path = os.path.join(SAVE_DIR, f"{code}.csv")
    existing_df = pd.read_csv(output_path, encoding="utf-8-sig") if os.path.exists(output_path) else pd.DataFrame()

    url = "https://www.tpex.org.tw/www/zh-tw/emerging/historical"
    columns_needed = ["日期", "成交股數", "成交金額", "成交最高", "成交最低", "成交均價", "成交筆數"]

    for i in range(months):
        month_offset = start_month - i
        year_offset = start_roc_year
        if month_offset <= 0:
            month_offset += 12
            year_offset -= 1

        y = year_offset + 1911
        date_str = f"{y}/{month_offset:02d}/01"

        payload = {"code": code, "date": date_str, "id": ""}
        headers = get_random_headers()

        try:
            res = safe_post(url, headers=headers, data=payload)
            json_data = res.json()
            table = json_data.get("tables", [{}])[0]
            data = table.get("data", [])

            if data:
                # 取出需要的欄位
                month_df = pd.DataFrame(
                    [[row[0], row[1], row[2], row[3], row[4], row[5], row[6]] for row in data],
                    columns=columns_needed
                )

                # 檢查日期是否已存在，避免重複
                if not existing_df.empty and "日期" in existing_df.columns:
                    month_df = month_df[~month_df["日期"].isin(existing_df["日期"])]

                if not month_df.empty:
                    existing_df = pd.concat([existing_df, month_df], ignore_index=True)

        except Exception as e:
            print(f"⚠️ [{code}] 抓取 {year_offset}年{month_offset:02d} 月失敗: {e}")

        time.sleep(random.uniform(1.0, 1.5))

    if not existing_df.empty:
        try:
            # 轉換日期方便排序（民國轉西元）
            df_tmp = existing_df.copy()
            df_tmp["日期_sort"] = pd.to_datetime(
                df_tmp["日期"].apply(lambda x: str(int(x.split("/")[0]) + 1911) + "/" + "/".join(x.split("/")[1:])),
                errors="coerce"
            )
            existing_df = df_tmp.sort_values(by="日期_sort", ascending=False).drop(columns=["日期_sort"]).reset_index(drop=True)
        except Exception as e:
            print(f"⚠️ 排序失敗：{e}")

        existing_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ [{code}] 資料已更新，共 {len(existing_df)} 筆")
    else:
        print(f"⚠️ [{code}] 無資料")

# === 包裝函式 ===
def fetch_task(code):
    now = datetime.now()
    current_roc_year = now.year - 1911
    current_month = now.month
    fetch_emerging_stock(code, current_roc_year, current_month, months=12)

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
        with tqdm(total=len(stock_codes), desc="興櫃股票進度") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ [{futures[future]}] 執行失敗：{e}")
                pbar.update(1)

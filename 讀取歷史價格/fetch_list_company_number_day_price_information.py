import requests
import pandas as pd
import os
import time
import random
from datetime import datetime
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
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": "https://www.twse.com.tw/zh/trading/historical/stock-day.html",
        "User-Agent": random.choice(USER_AGENTS),
        "X-Requested-With": "XMLHttpRequest",
    }

# === 請求封裝 ===
def safe_get(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            return res
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                raise e

# === 抓取單一股票資料（半年內） ===
def fetch_twse_stock(code: str, start_year: int, start_month: int, months: int = 6):
    output_path = os.path.join(SAVE_DIR, f"{code}.csv")
    existing_df = pd.read_csv(output_path, encoding="utf-8-sig") if os.path.exists(output_path) else pd.DataFrame()

    url_template = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={date_str}&stockNo={code}&response=json"

    # 準備已存在的月份 (YYYY-MM 格式)
    existing_months = set()
    if not existing_df.empty:
        date_col = existing_df.columns[0]  # 假設第一欄就是日期
        try:
            # 民國轉西元 (114/08/15 → 2025/08/15)
            existing_df["日期_西元"] = pd.to_datetime(
                existing_df[date_col].apply(lambda x: str(int(x.split("/")[0]) + 1911) + "/" + "/".join(x.split("/")[1:])),
                format="%Y/%m/%d",
                errors="coerce"
            )
            existing_months = set(existing_df["日期_西元"].dropna().dt.strftime("%Y-%m"))
        except Exception as e:
            print(f"⚠️ [{code}] 日期格式轉換失敗: {e}")

    for i in range(months):
        month_offset = start_month - i
        year_offset = start_year
        if month_offset <= 0:
            month_offset += 12
            year_offset -= 1

        y = year_offset
        ym_str = f"{y}-{month_offset:02d}"
        date_str = f"{y}{month_offset:02d}01"

        if ym_str in existing_months:
            continue

        url = url_template.format(date_str=date_str, code=code)

        try:
            res = safe_get(url)
            json_data = res.json()
            data = json_data.get("data", [])
            fields = json_data.get("fields", [])

            if data:
                filtered_data = [row for row in data if "--" not in row]

                if filtered_data:
                    month_df = pd.DataFrame(filtered_data, columns=fields)

                    # 新增西元日期欄位，方便排序
                    month_df["日期_西元"] = pd.to_datetime(
                        month_df["日期"].apply(lambda x: str(int(x.split("/")[0]) + 1911) + "/" + "/".join(x.split("/")[1:])),
                        format="%Y/%m/%d",
                        errors="coerce"
                    )

                    existing_df = pd.concat([existing_df, month_df], ignore_index=True)
                    existing_df.drop_duplicates(inplace=True)

                    existing_months = set(existing_df["日期_西元"].dropna().dt.strftime("%Y-%m"))
        except Exception as e:
            print(f"⚠️ [{code}] 抓取 {ym_str} 失敗: {e}")

        time.sleep(3)

    if not existing_df.empty:
        try:
            # 用西元日期排序，從新到舊
            existing_df = existing_df.sort_values(by="日期_西元", ascending=False, ignore_index=True)

            # 轉回民國日期格式存檔
            existing_df["日期"] = existing_df["日期_西元"].dt.strftime("%Y/%m/%d").apply(
                lambda x: f"{int(x.split('/')[0]) - 1911}/{x.split('/')[1]}/{x.split('/')[2]}"
            )

            # 移除臨時的西元日期欄位
            existing_df = existing_df.drop(columns=["日期_西元"])
        except Exception as e:
            print(f"⚠️ 排序或轉換民國日期失敗: {e}")

        existing_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ [{code}] 資料已更新")
    else:
        print(f"⚠️ [{code}] 無資料")

    time.sleep(3)


# === 主程式（單執行緒 + 進度條）===
if __name__ == "__main__":
    try:
        stock_codes = read_stock_codes()
    except Exception as e:
        print(f"讀取股票代號失敗：{e}")
        exit(1)

    now = datetime.now()
    current_year = now.year
    current_month = now.month

    with tqdm(total=len(stock_codes), desc="股票進度") as pbar:
        for code in stock_codes:
            try:
                fetch_twse_stock(code, current_year, current_month, months=6)
            except Exception as e:
                print(f"❌ [{code}] 執行失敗：{e}")
            pbar.update(1)

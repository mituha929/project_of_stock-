import os
import requests
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor

def fetch_stock_data(mode, output_filename, valid_cfi_prefixes, output_dir="output"):
    # 確保資料夾存在
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, output_filename)

    url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    response.encoding = "big5-hkscs"

    stocks = []
    stock_id_list = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.find_all("tr")

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 6:
                raw = cols[0].text.strip()
                cfi_code = cols[5].text.strip()

                if any(cfi_code.startswith(prefix) for prefix in valid_cfi_prefixes):
                    cleaned = " ".join(raw.split())
                    parts = cleaned.split(" ", 1)

                    if len(parts) == 2:
                        stock_id = parts[0]
                        stock_name = parts[1]
                        listing_date = cols[2].text.strip()
                        market_type = cols[3].text.strip()
                        industry = cols[4].text.strip()

                        stocks.append([
                            stock_id,
                            stock_name,
                            listing_date,
                            market_type,
                            industry,
                        ])
                        stock_id_list.append(stock_id)

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["股票代號", "股票名稱", "上市日", "市場別", "產業別"])
            writer.writerows(stocks)

        print(f"✅ 已成功儲存 {len(stocks)} 筆資料至 {filepath}")
    else:
        print(f"❌ 連線失敗：HTTP {response.status_code}，網址：{url}")

    return stock_id_list

# 包裝函式給 ThreadPoolExecutor 使用
def fetch_stock_data_thread(args):
    return fetch_stock_data(*args)

output_folder = os.path.join("data")

# 任務清單：加入 output_dir 作為第 4 個參數
tasks = [
    (2, "list_company_number.csv", ('ESV', 'CEO', 'CMX', 'EDS', 'CBC', 'EF', 'EP'), output_folder),
    (4, "over_the_counter_number.csv", ('ESV', 'CEO', 'CMX', 'EPN'), output_folder),
    (5, "emerging_stock_market.csv", ('ESV',), output_folder),
]

# 執行任務
all_stock_ids = []
with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(fetch_stock_data_thread, tasks)
    for stock_ids in results:
        all_stock_ids.extend(stock_ids)

# 去除重複並示範前10筆
all_stock_ids = list(set(all_stock_ids))
print("📌 前10個股票代號：", all_stock_ids[:10])

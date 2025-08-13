import requests
import csv

# 股票代號
stock_no = "2330"
# 日期（YYYYMMDD）
date = "20250813"

# API URL
url = f"https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date={date}&stockNo={stock_no}&response=json"

# 發送 GET 請求
response = requests.get(url)
data = response.json()

# 欲輸出的欄位標題
headers = ["日期", "成交股數", "成交金額", "成交最高", "成交最低", "成交均價", "成交筆數"]

rows = []
for item in data["data"]:
    date_str = item[0]  # 日期
    volume = int(item[1].replace(",", ""))  # 成交股數
    amount = int(item[2].replace(",", ""))  # 成交金額
    high = float(item[4].replace(",", ""))  # 最高價
    low = float(item[5].replace(",", ""))   # 最低價
    avg_price = round(amount / volume, 2)   # 均價（四捨五入到小數兩位）
    trades = int(item[8].replace(",", ""))  # 成交筆數

    rows.append([date_str, volume, amount, high, low, avg_price, trades])

# 輸出 CSV
filename = f"{stock_no}.csv"
with open(filename, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

print(f"已儲存至 {filename}")

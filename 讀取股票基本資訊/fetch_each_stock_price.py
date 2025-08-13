import requests
from bs4 import BeautifulSoup
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm  # ✅ 新增進度條模組

# 資料夾路徑
DATA_FOLDER = os.path.join("data")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "zh-TW,zh;q=0.9"
}

def fetch_price_cnyes(row, url_index, price_index, change_index, percent_index, source_index):
    try:
        url = row[url_index]
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, "html.parser")
        price = soup.find("h3", class_="jsx-2312976322")
        spans = soup.find("div", class_="first-row")
        span_tags = spans.find_all("span") if spans else []

        row[price_index] = price.text.strip() if price else ""
        row[change_index] = span_tags[0].text.strip() if len(span_tags) > 0 else ""
        row[percent_index] = span_tags[1].text.strip() if len(span_tags) > 1 else ""

        if all([row[price_index], row[change_index], row[percent_index]]):
            row[source_index] = "鉅亨"
            return True
        return False
    except Exception as e:
        print(f"[鉅亨錯誤] {row[0]}: {e}")
        return False

def fetch_price_cmoney(stock_id, row, price_index, change_index, percent_index, source_index):
    try:
        url = f"https://www.cmoney.tw/forum/stock/{stock_id}"
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, "html.parser")

        price = soup.find("div", class_="stockData__price")
        change = soup.find("div", class_="stockData__quotePrice")
        percent = soup.find("div", class_="stockData__quote")

        row[price_index] = price.text.strip() if price else ""
        row[change_index] = change.text.strip() if change else ""
        row[percent_index] = percent.text.strip() if percent else ""

        if all([row[price_index], row[change_index], row[percent_index]]):
            row[source_index] = "CMoney"
            return True
        return False
    except Exception as e:
        print(f"[CMoney錯誤] {row[0]}: {e}")
        return False

def fetch_price_pchome(stock_id, row, price_index, change_index, percent_index, source_index):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--enable-unsafe-swiftshader")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    try:
        url = f"https://pchome.megatime.com.tw/stock/sid{stock_id}.html"
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "data_close")))
        soup = BeautifulSoup(driver.page_source, "html.parser")

        price = soup.find("span", class_="data_close")
        change = soup.find_all("span", class_="data_diff")

        row[price_index] = price.text.strip() if price else ""
        row[change_index] = change[0].text.strip() if len(change) > 0 else ""
        row[percent_index] = change[1].text.strip() if len(change) > 1 else ""

        if all([row[price_index], row[change_index], row[percent_index]]):
            row[source_index] = "PChome"
            return True
        return False
    except Exception as e:
        print(f"[PChome錯誤] {row[0]}: {e}")
        return False
    finally:
        driver.quit()

def ensure_field(header, field):
    if field not in header:
        header.append(field)
    return header.index(field)

def fill_missing_fields(row, length):
    while len(row) < length:
        row.append("")

def run(filename):
    filepath = os.path.join(DATA_FOLDER, filename)

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    url_index = header.index("鉅亨網網址")
    market_index = header.index("市場別")
    price_index = ensure_field(header, "價格")
    change_index = ensure_field(header, "漲跌")
    percent_index = ensure_field(header, "漲跌幅度(%)")
    source_index = ensure_field(header, "資料來源")

    target_len = len(header)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for row in rows:
            fill_missing_fields(row, target_len)
            stock_id = row[0]
            market = row[market_index].strip()

            def task(row=row, stock_id=stock_id):
                if fetch_price_cnyes(row, url_index, price_index, change_index, percent_index, source_index):
                    return row
                if fetch_price_cmoney(stock_id, row, price_index, change_index, percent_index, source_index):
                    return row
                if fetch_price_pchome(stock_id, row, price_index, change_index, percent_index, source_index):
                    return row

            futures.append(executor.submit(task))

        # ✅ 使用 tqdm 包裝 as_completed，顯示進度條
        for future in tqdm(as_completed(futures), total=len(futures), desc="📈 更新中"):
            future.result()

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    print(f"\n✅ 更新完成：{filename}")

# ✅ 呼叫（範例）
run("list_company_number.csv")
run("over_the_counter_number.csv")
run("emerging_stock_market.csv")

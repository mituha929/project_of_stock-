import requests
import json

url = "https://www.tpex.org.tw/www/zh-tw/emerging/historical"
#url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
payload = {
    "code": "6035",
    "date": "2025/08/01",
    "id": ""
}
headers = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent": "Mozilla/5.0"
}

res = requests.post(url, headers=headers, data=payload)
print(json.dumps(res.json(), ensure_ascii=False, indent=2))

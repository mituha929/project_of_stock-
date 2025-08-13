import csv
import os

def fetch_cnyes_stock_link(input_file):
    output_rows = []
    with open(input_file, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)

        # 判斷是否已有「鉅亨網網址」欄位
        if "鉅亨網網址" in header:
            url_index = header.index("鉅亨網網址")
        else:
            url_index = len(header)
            header.append("鉅亨網網址")

        output_rows.append(header)

        for row in reader:
            # 若資料長度不足補齊
            if len(row) <= url_index:
                row += [""] * (url_index + 1 - len(row))

            stock_id = row[0].zfill(4)
            cnyes_url = f"https://www.cnyes.com/twstock/{stock_id}"

            row[url_index] = cnyes_url
            output_rows.append(row)

    # 寫回原 CSV 檔案（覆寫）
    with open(input_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerows(output_rows)

    print(f"✅ 已成功更新鉅亨網網址至 {input_file}")

# ✅ 一次處理多個檔案
file_list = [
    "list_company_number.csv",
    "emerging_stock_market.csv",
    "over_the_counter_number.csv"
]

# 根據 output_folder 建立完整路徑並處理每個檔案
for file in file_list:
    output_folder = os.path.join("data")
    full_path = os.path.join(output_folder, file)
    fetch_cnyes_stock_link(full_path)


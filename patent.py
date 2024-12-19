# Download patent data from the Taiwan Intellectual Property Office API and store the results in a CSV file.

import os
import csv
from datetime import datetime, timedelta
import json
import time
import requests

with open('company-list.csv', 'r', encoding='utf-8-sig') as f:
    companies = list(csv.reader(f))

api_url = 'https://gpss1.tipo.gov.tw/gpsskmc/gpss_api'
params = {
    'userCode': 'xxxxxxxxxxxxxxx', # Replace this 
    'patDB': 'TWA,TWB,TWD,JPA,JPB,JPD,CNA,CNB,CND,KPA,KPB,KPD,USA,USB,USD,SEAA,SEAB,WO,EPA,EPB,EUIPO,OTA,OTB',
    'patAG': 'A,B',
    'patTY': 'I,M,D',
    'expFld': 'PN,AN,ID,AD,TI,PA,IC',
    'expFmt': 'json',
    'expQty': '1',
}

timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
new_dir = f'results/{timestamp}'
os.makedirs(f'{new_dir}/json', exist_ok=True)

types_AG = params['patAG'].split(',')
types_TY = params['patTY'].split(',')

patent_counts = {}

try:
    with open(f'{new_dir}/patent-result.csv', 'w', encoding='utf-8') as fout:
        writer = csv.writer(fout)
        writer.writerow(['年份', '公司代碼', '公開發明', '公開新型', '公開設計', '公告發明', '公告新型', '公告設計', '加總'])
        
    for year in range(2015, 2016):
        params['ID'] = f'{year}0101:{year}1231'

        for i, row in enumerate(companies):
            code, name = row[0], row[1]
            params['AX'] = name

            for type_AG in types_AG:
                params['patAG'] = type_AG

                for type_TY in types_TY:
                    params['patTY'] = type_TY

                    while True:
                        resp = requests.get(api_url, params=params)
                        data = resp.json()
                        with open(f'{new_dir}/json/{code}_{year}_{type_AG}_{type_TY}.json', 'w', encoding='utf-8') as json_file:
                            json.dump(data, json_file, ensure_ascii=False, indent=4)

                        if 'message' in data['gpss-API'] and data['gpss-API']['message'] == 'Over download quantity':
                            fail_time = datetime.now()
                            next_hour = (fail_time.replace(minute=0, second=0) + timedelta(hours=1)).time()
                            remaining_time = datetime.combine(datetime.today(), next_hour) - fail_time

                            print(f"\rAPI 失敗於 {fail_time.strftime('%Y-%m-%d %H:%M:%S')}, 下一小時於 {next_hour}, 剩餘時間為 {remaining_time.seconds // 60} 分鐘 和 {remaining_time.seconds % 60} 秒鐘.", end='')
                            time.sleep(5)
                        else:
                            break

                    total_patents = int(data['gpss-API']['total-rec']) if 'total-rec' in data['gpss-API'] and isinstance(data['gpss-API']['total-rec'], str) else 0

                    patent_counts.setdefault(year, {}).setdefault(code, {})[f'{type_AG}{type_TY}'] = total_patents

                    print(f'年份:{year}, 進度:{i+1}/{len(companies)}, 案件類型:{type_AG}, 專利類型:{type_TY}')

            with open(f'{new_dir}/patent-result.csv', 'a', encoding='utf-8') as fout:
                writer = csv.writer(fout)
                for year, company_data in patent_counts.items():
                    for company, patent_data in company_data.items():
                        total = sum(patent_data.values())
                        writer.writerow([year, company, patent_data.get('AI', 0), patent_data.get('AM', 0), patent_data.get('AD', 0), patent_data.get('BI', 0), patent_data.get('BM', 0), patent_data.get('BD', 0), total])
                patent_counts.clear()

        print(f'{year}年查詢完成')

except KeyboardInterrupt:
    print("程式被中斷")
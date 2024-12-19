import csv
import requests
import time
import os
from tqdm import tqdm

url_prefix = 'https://mops.twse.com.tw/server-java/FileDownLoad?step=9&filePath=/home/html/nas/protect/t100/&fileName='

# Taiwan year + 1911 = Gregorian calendar.
for year in range(101, 112):
    filename = str(year) + '.csv'

    if not os.path.exists(filename):
        print(f"Skip {year}.")
        continue

    folder_name = "CSR_" + str(year)
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

#cp950 for excel

    with open(filename, 'r', encoding='cp950') as f:
        reader = csv.DictReader(f)
        total_rows = sum(1 for row in f)
        f.seek(0)

        for i, row in enumerate(reader, start=1):
            if row['中文版永續報告書(修正後版本)']:
                link = url_prefix + row['中文版永續報告書(修正後版本)']
            else:
                link = url_prefix + row['中文版永續報告書']

            while True:
                file_name = link.split('=')[-1]
                file_path = os.path.join(folder_name, file_name)

                if os.path.exists(file_path) and (os.path.getsize(file_path) > 524288):
                    print(f" {file_name} has been downloaded, skip...")
                    break

                print(f"Downloading: {file_name}")

                try:
                    response = requests.get(link, stream=True)
                    response.raise_for_status()

                    with open(file_path, 'wb') as output_file:
                        progress_bar = tqdm(total=int(response.headers.get('content-length', 0)), unit='iB', unit_scale=True)
                        for data in response.iter_content(chunk_size=1024):
                            output_file.write(data)
                            progress_bar.update(len(data))
                        progress_bar.close()

                    total_size = os.path.getsize(file_path)

                    if total_size < 524288:
                        with open(file_path, 'rb') as check_file:
                            content = check_file.read().decode('utf-8', 'ignore')

                            if "Too many query requests from your ip" in content:
                                for i in range(10, 0, -1):
                                    time.sleep(1)
                                    file_size = os.path.getsize(file_path)
                                    print(f"Rate limits, waiting: {i} seconds", end='\r', flush=True)
                                continue

                            elif file_name.endswith('_M.pdf'):
                                print(f"The file {file_name} is broken, re-downloading...")
                                link = url_prefix + file_name.replace('_M.pdf', '.pdf')
                                file_size = os.path.getsize(file_path)
                                os.remove(file_path)
                                print(f"Removed {file_path} of size {file_size} bytes.")
                                continue
                    break

                except requests.HTTPError as err:
                    print(f"HTTP error occurred for {file_path}: {err}")
                except Exception as err:
                    print(f"Unknown error occurred for {file_path}: {err}")

                time.sleep(1)

    print(f"Total Progress: {i}/{total_rows}")
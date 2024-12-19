import os
import csv
import pdfplumber

def load_keywords(csv_path):
    with open(csv_path, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        keywords = {field: [] for field in reader.fieldnames}
        for row in reader:
            for field in reader.fieldnames:
                if row[field]:
                    keywords[field].append(row[field])
    return keywords

def search_keywords_in_pdf(pdf_path, keywords):
    keyword_counts = {keyword: 0 for keyword in keywords}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    for keyword in keywords:
                        keyword_counts[keyword] += page_text.count(keyword)
    except Exception as e:
        print(f"Failed to process file {pdf_path} due to error: {e}")

    return keyword_counts

def save_results(result, output_csv_path, keywords_dict):
    with open(output_csv_path, 'a', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        for stock_code, pdf_data in result.items():
            for pdf_file, counts in pdf_data.items():
                row = [stock_code] + [counts.get(keyword, 0) for keyword in all_keywords] + \
                    [counts.get(f'SUM_{field}', 0) for field in keywords_dict]
                writer.writerow(row)

csv_path = 'keywords.csv'
keywords_dict = load_keywords(csv_path)

all_keywords = [keyword for sublist in keywords_dict.values() for keyword in sublist]

years = [105, 106, 107, 108, 109]

for year in years:

  pdf_folder = f'original-pdfs/CSR_{year}'
  output_csv_path = os.path.join(pdf_folder, f'{year}_result.csv')

  #CSV header
  with open(output_csv_path, 'w', newline='', encoding='utf-8-sig') as file:
    writer = csv.writer(file)
    writer.writerow(['Stock Code'] + all_keywords + [f'SUM_{field}' for field in keywords_dict])

  pdf_files = sorted([f for f in os.listdir(pdf_folder) if f.endswith('.pdf')])
  total_files = len(pdf_files)

  for i, filename in enumerate(pdf_files, start=1):
    pdf_path = os.path.join(pdf_folder, filename)
    stock_code = filename.split('_')[1]
    counts = search_keywords_in_pdf(pdf_path, all_keywords)
    

    for field, keywords in keywords_dict.items():
      counts[f'SUM_{field}'] = sum(counts.get(keyword, 0) for keyword in keywords)

    result = {stock_code: {filename: counts}}
    save_results(result, output_csv_path, keywords_dict)

    print(f'Processed {year} {i}/{total_files}: {filename}')

print('All done.')
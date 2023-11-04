import gspread
from apollo import Apollo
import threading
import time
import json

gc = gspread.service_account()
sh = gc.open("Sourcing Companies [Master]")
worksheet = sh.worksheet("Automation")

running_thread_names = {}

def run_apollo_on_column(key, name):
    companies_to_do = running_thread_names[name]
    for company in companies_to_do:
        url, row, status_col = company
        try:
            worksheet.update_cell(row, status_col, 'Working on it...')
            apollo_client = Apollo(key)

            successfully_added = apollo_client.get_and_add_people(url)

            worksheet.update_cell(row, status_col, 'Completed!')
        except:
            worksheet.update_cell(row, status_col, 'Error')

    running_thread_names.pop(name, None)

while True:
    try:
        users = []
        
        with open('keys.json', 'r') as fp:
            keys = json.load(fp)
        
        # For each val in the first and second rows, get users + keys
        for name in worksheet.row_values(1):
            if name not in keys:
                continue
            users.append((name, keys.get(name)))

        # For each user, get a list of companies that are yet to be done. Then, do them!
        for i, user in enumerate(users):
            name, key = user
            if name in running_thread_names:
                continue
            
            col = 2*i + 2

            companies = list(worksheet.col_values(col))
            companies = companies[1:]

            companies_to_do = []

            for j, company in enumerate(companies):
                row = j+2
                status_col = col+1
                url = company
                if not url:
                    continue
                val = worksheet.cell(row, status_col).value or ''

                if 'Completed' in val:
                    continue
                companies_to_do.append((url, row, status_col))

            if companies_to_do:
                thread = threading.Thread(target=run_apollo_on_column, args=(key, name,))
                running_thread_names[name] = companies_to_do
                thread.start()
        
        print(f"[LOG] {time.strftime('%X %x %Z')}", running_thread_names)
        time.sleep(20)
    except Exception as e:
        print(e)
        time.sleep(20)
        continue

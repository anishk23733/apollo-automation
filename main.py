from apollo import Apollo

API_KEY = ''

if __name__ == '__main__':
    apollo_client = Apollo(API_KEY)
    company_url = 'tome.app'

    successfully_added = apollo_client.get_and_add_people(company_url)

    print(f'[LOG] Successfully added {successfully_added} contacts.')

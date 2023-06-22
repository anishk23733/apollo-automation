from apollo import Apollo
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add people from a company URL.')
    parser.add_argument('company_url', help='The company URL to fetch people from.')

    args = parser.parse_args()

    api_key = os.environ.get('API_KEY')
    apollo_client = Apollo(api_key)
    company_url = args.company_url

    successfully_added = apollo_client.get_and_add_people(company_url)

    print(f'[LOG] Successfully added {successfully_added} contacts.')

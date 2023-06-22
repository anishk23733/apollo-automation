from apollo import Apollo
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Add people from a company URL.')
    parser.add_argument('company_url', nargs='+', default=[], help='The company URLs to fetch people from.')

    args = parser.parse_args()

    api_key = os.environ.get('API_KEY')
    company_urls = args.company_url

    apollo_client = Apollo(api_key)
    
    for url in company_urls:
        successfully_added = apollo_client.get_and_add_people(url)

        print(f'[LOG] Successfully added {successfully_added} contacts for {url}.')

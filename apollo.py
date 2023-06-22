import requests
import tqdm
import time
import json
import os

class Apollo():
    MINUTE_LIMIT = 50
    HOUR_LIMIT = 200
    DAY_LIMIT = 600
    PER_REQUEST_WAIT = 50/60

    def __init__(self, api_key):
        self.api_key = api_key
        
        self.window_start = time.time()
        self.window_requests = 0

        self.send_email_from_email_account_id = self.get_email_account_id()
        self.emailer_campaign_id = self.get_sequence_id()

    def handle_request(self, url, data, type_='post'):
        if type_ == 'post':
            res = requests.post(
                url,
                json=data
            )
        elif type_ == 'get':
            res = requests.get(
                url,
                params=data
            )
        
        time.sleep(Apollo.PER_REQUEST_WAIT)
        self.window_requests += 1
        
        if self.window_requests >= Apollo.MINUTE_LIMIT - 5 and time.time() - self.window_start < 60:
            self.window_requests = 0
            # wait till the next minute
            print('[LOG] Sleeping for', 60 - (time.time() - self.window_start), 'seconds till the next minute.')
            time.sleep(60 - (time.time() - self.window_start))
            self.window_start = time.time()
        elif self.window_requests >= Apollo.HOUR_LIMIT - 5 and time.time() - self.window_start < 3600:
            self.window_requests = 0
            # wait till the next hour
            print('[LOG] Sleeping for', 3600 - (time.time() - self.window_start), 'seconds till the next hour.')
            time.sleep(3600 - (time.time() - self.window_start))
            self.window_start = time.time()
        elif self.window_requests >= Apollo.DAY_LIMIT - 5 and time.time() - self.window_start < 86400:
            self.window_requests = 0
            # wait till the next day
            print('[LOG] Sleeping for', 86400 - (time.time() - self.window_start), 'seconds till the next day.')
            time.sleep(86400 - (time.time() - self.window_start))
            self.window_start = time.time()
        
        if res.status_code == 429:
            headers = res.headers
            retry_after = int(headers['retry-after']) + 20
            print(f'[LOG] Rate limited, sleeping for {retry_after} seconds or {retry_after/60} minutes.')
            time.sleep(retry_after)
        elif res.status_code == 500:
            print('[LOG] Internal server error, sleeping for 60 seconds.')
            time.sleep(60)
        elif res.status_code == 502:
            print('[LOG] Bad gateway, sleeping for 60 seconds. Did you provide a correct key?')
            time.sleep(60)
        elif res.status_code != 200:
            print('[LOG] Unknown error, sleeping for 60 seconds. Did you provide a correct key?')
            time.sleep(60)
        else:
            return res
            
        return self.handle_request(url, data)

    def get_email_account_id(self):
        res = self.handle_request('https://api.apollo.io/v1/email_accounts', {'api_key': self.api_key}, type_='get')

        res = res.json()
        ids = []
        for email_account in res['email_accounts']:
            ids.append(email_account['id'])
        
        assert len(ids) == 1, "More/less than one email attached to account"
        return ids[0]

    def get_and_add_people(self, company_url):
        batch_size = 50
        added = 0

        roles = ['Director', 'Manager', 'VP', 'CEO', 'Founder']
        res = self.handle_request(
            'https://api.apollo.io/v1/mixed_people/search',
            {
                'person_titles': roles,
                'q_organization_domains': company_url,
                'page': 1,
                'api_key': self.api_key
            },
            type_='post'
        )

        res = res.json()
        total_pages = res['pagination']['total_pages']

        people_ids = []
        if os.path.exists('cache.json'):
            with open('cache.json', 'r') as f:
                people_ids = json.load(f)

        for person in res['people']:
            new_id = self.create_contact(person)
            if not new_id:
                print('Failed to make', person)
                continue
            people_ids.append(new_id)

            with open('cache.json', 'w') as f:
                json.dump(people_ids, f)
            
            if len(people_ids) >= batch_size:
                res2 = self.add_contacts_to_sequence(
                    people_ids
                )
                added += len(res2['contacts'])

                people_ids = []
                with open('cache.json', 'w') as f:
                    json.dump(people_ids, f)
        print(f'[LOG] Total pages of contacts is {total_pages}.')

        for page in tqdm.tqdm(range(2, total_pages + 1)):
            res2 = self.handle_request(
                'https://api.apollo.io/v1/mixed_people/search',
                {
                    'person_titles': roles,
                    'q_organization_domains': company_url,
                    'page': 1,
                    'api_key': self.api_key
                }, 
                type_='post'
            )

            res2 = res2.json()
            for person in res2['people']:
                new_id = self.create_contact(person)
                if not new_id:
                    continue
                people_ids.append(new_id)
                
                with open('cache.json', 'w') as f:
                    json.dump(people_ids, f)
                
                if len(people_ids) >= batch_size:
                    res3 = self.add_contacts_to_sequence(
                        people_ids,
                    )
                    added += len(res3['contacts'])

                    people_ids = []
                    with open('cache.json', 'w') as f:
                        json.dump(people_ids, f)
        
        if len(people_ids) > 0:
            res2 = self.add_contacts_to_sequence(
                people_ids,
            )
            added += len(res2['contacts'])
        
        return added

    def get_sequence_id(self):
        res = self.handle_request(
            'https://api.apollo.io/v1/emailer_campaigns/search',
            {
                'api_key': self.api_key
            },
            type_='post'
        )

        res = res.json()
        ids = []

        for campaign in res['emailer_campaigns']:
            ids.append(campaign['id'])
        
        assert len(ids) == 1, 'More/less than one sequence found'
        return ids[0]

    def add_contacts_to_sequence(self, contact_ids):
        res = self.handle_request(
            f'https://api.apollo.io/v1/emailer_campaigns/{self.emailer_campaign_id}/add_contact_ids',
            {
                'contact_ids': contact_ids,
                'emailer_campaign_id': self.emailer_campaign_id,
                'send_email_from_email_account_id': self.send_email_from_email_account_id,
                'api_key': self.api_key
            },
            type_='post'
        )

        return res.json()

    def continue_with_contact(self, email):
        res = self.handle_request(
            'https://api.apollo.io/v1/contacts/search',
            {
                'api_key': self.api_key,
                'q_keywords': f'{email}',
            },
            type_='post'
        )
        
        res = res.json()
        
        if len(res['contacts']) == 0:
            return True
        else:
            return False

    def create_contact(self, contact):
        if not contact.get('email'):
            return None
        
        res = self.handle_request(
            'https://api.apollo.io/v1/contacts',
            {
                'api_key': self.api_key,
                # 'first_name': contact['first_name'],
                # 'last_name': contact['last_name'],
                # 'organization_name': contact['organization']['name'],
                # 'title': contact['title'],
                'email': contact['email'],
                # 'website_url': contact['organization']['website_url'],
                # 'account_id': contact['id'],
            },
            type_='post'
        )
        
        res = res.json()
        return res['contact']['id']

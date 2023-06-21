import requests
import tqdm
import time
import json
import os

API_KEY = ''

MINUTE_LIMIT = 50
HOUR_LIMIT = 200
DAY_LIMIT = 600

PER_REQUEST_WAIT = 50/60

recovery_cache = []

window_requests = 0
window_start = time.time()

def handle_request(url, json_params, type_='post'):
    global window_requests, window_start
    if type_ == 'post':
        res = requests.post(
            url,
            json=json_params
        )
    elif type_ == 'get':
        res = requests.get(
            url,
            params=json_params
        )
    
    time.sleep(PER_REQUEST_WAIT)
    window_requests += 1
    if window_requests >= MINUTE_LIMIT - 5 and time.time() - window_start < 60:
        window_requests = 0
        # wait till the next minute
        print('Sleeping for', 60 - (time.time() - window_start), 'seconds till the next minute')
        time.sleep(60 - (time.time() - window_start))
        window_start = time.time()
    if window_requests >= HOUR_LIMIT - 5 and time.time() - window_start < 3600:
        window_requests = 0
        # wait till the next hour
        print('Sleeping for', 3600 - (time.time() - window_start), 'seconds till the next hour')
        time.sleep(3600 - (time.time() - window_start))
        window_start = time.time()
    if window_requests >= DAY_LIMIT - 5 and time.time() - window_start < 86400:
        window_requests = 0
        # wait till the next day
        print('Sleeping for', 86400 - (time.time() - window_start), 'seconds till the next day')
        time.sleep(86400 - (time.time() - window_start))
        window_start = time.time()

    if res.status_code == 429:
        headers = res.headers
        retry_after = int(headers['retry-after']) + 20
        print(f'Rate limited, sleeping for {retry_after} seconds or {retry_after/60} minutes')
        time.sleep(retry_after)
    elif res.status_code == 500:
        print('Internal server error, sleeping for 60 seconds')
        time.sleep(60)
    elif res.status_code == 502:
        print('Bad gateway, sleeping for 60 seconds')
        time.sleep(60)
    elif res.status_code != 200:
        print('Unknown error, sleeping for 60 seconds')
        time.sleep(60)
    else:
        return res
        
    return handle_request(url, json_params)

def get_email_account_id():
    res = handle_request('https://api.apollo.io/v1/email_accounts', {'api_key': API_KEY}, type_='get')

    res = res.json()
    id_ = None
    for email_account in res['email_accounts']:
        if 'berkeley.edu' in email_account['email']:
            id_ = email_account['id']
    
    return id_

def get_and_add_people(company_url, emailer_campaign_id, send_email_from_email_account_id):
    global recovery_cache
    batch_size = 50
    added = 0

    if os.path.exists('cache.json'):
        with open('cache.json', 'r') as f:
            recovery_cache = json.load(f)

    roles = ['Director', 'Manager', 'VP', 'CEO', 'Founder']
    res = handle_request(
        'https://api.apollo.io/v1/mixed_people/search',
        {
            'person_titles': roles,
            'q_organization_domains': company_url,
            'page': 1,
            'api_key': API_KEY
        },
        type_='post'
    )

    res = res.json()

    people_ids = recovery_cache

    for person in res['people']:
        new_id = create_contact(person)
        if not new_id:
            continue
        people_ids.append(new_id)

        recovery_cache.append(new_id)
        with open('cache.json', 'w') as f:
            json.dump(recovery_cache, f)
        
        if len(people_ids) == batch_size:
            res = add_contacts_to_sequence(
                emailer_campaign_id,
                people_ids,
                send_email_from_email_account_id
            )
            added += len(res['contacts'])

            recovery_cache = []
            with open('cache.json', 'w') as f:
                json.dump(recovery_cache, f)

            people_ids = []

    total_pages = res['pagination']['total_pages']
    
    print('Total pages of contacts is', total_pages)
    for page in tqdm.tqdm(range(2, total_pages + 1)):
        res = handle_request(
            'https://api.apollo.io/v1/mixed_people/search',
            {
                'person_titles': roles,
                'q_organization_domains': company_url,
                'page': 1,
                'api_key': API_KEY
            }, 
            type_='post'
        )

        res = res.json()
        for person in res['people']:
            new_id = create_contact(person)
            if not new_id:
                continue
            people_ids.append(new_id)
            
            recovery_cache.append(new_id)
            with open('cache.json', 'w') as f:
                json.dump(recovery_cache, f)
            
            if len(people_ids) == batch_size:
                res = add_contacts_to_sequence(
                    emailer_campaign_id,
                    people_ids,
                    send_email_from_email_account_id
                )
                added += len(res['contacts'])

                recovery_cache = []
                with open('cache.json', 'w') as f:
                    json.dump(recovery_cache, f)
                
                people_ids = []
    
    if len(people_ids) > 0:
        res = add_contacts_to_sequence(
            emailer_campaign_id,
            people_ids,
            send_email_from_email_account_id
        )
        added += len(res['contacts'])
    
    return True

def get_sequence_id():
    res = handle_request(
        'https://api.apollo.io/v1/emailer_campaigns/search',
        {
            'api_key': API_KEY
        },
        type_='post'
    )

    res = res.json()
    ids = []

    for campaign in res['emailer_campaigns']:
        ids.append(campaign['id'])
    
    assert len(ids) == 1, 'More/less than one sequence found'
    return ids[0]

def add_contacts_to_sequence(emailer_campaign_id, contact_ids, send_email_from_email_account_id):
    res = handle_request(
        f'https://api.apollo.io/v1/emailer_campaigns/{emailer_campaign_id}/add_contact_ids',
        {
            'contact_ids': contact_ids,
            'emailer_campaign_id': emailer_campaign_id,
            'send_email_from_email_account_id': send_email_from_email_account_id,
            'api_key': API_KEY
        },
        type_='post'
    )

    return res.json()

def continue_with_contact(email):
    res = handle_request(
        'https://api.apollo.io/v1/contacts/search',
        {
            'api_key': API_KEY,
            'q_keywords': f'{email}',
        },
        type_='post'
    )
    
    res = res.json()
    
    if len(res['contacts']) == 0:
        return True
    else:
        return False

def create_contact(contact):
    if not contact.get('organization'):
        return None
    elif not contact['organization'].get('website_url'):
        return None
    elif not contact.get('email'):
        return None
    elif not contact.get('first_name'):
        return None
    elif not contact.get('last_name'):
        return None
    elif not contact.get('title'):
        return None
    
    res = handle_request(
        'https://api.apollo.io/v1/contacts',
        {
            'api_key': API_KEY,
            'first_name': contact['first_name'],
            'last_name': contact['last_name'],
            'organization_name': contact['organization']['name'],
            'title': contact['title'],
            'email': contact['email'],
            'website_url': contact['organization']['website_url'],
            'account_id': contact['id'],

        },
        type_='post'
    )
    
    res = res.json()
    return res['contact']['id']

send_email_from_email_account_id = get_email_account_id()
emailer_campaign_id = get_sequence_id()

res = get_and_add_people(
    'salesforce.com',
    emailer_campaign_id,
    send_email_from_email_account_id
)

successfully_added = len(res['contacts'])
print(f'Successfully added {successfully_added}')

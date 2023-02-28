import os
import requests
import json
import random
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

TOKEN = os.getenv('HS_TOKEN')
BASE_URL = 'https://api.hubapi.com/crm/v3/objects'
headers = {
    'Authorization': f'Bearer {TOKEN}',
    'Content-Type': 'application/json'
}

class HubspotBatchErrorHandler:
    def __init__(self, object_type, payload, response, success_code) -> None:
        self.object_type = object_type
        self.payload = payload
        self.response = response
        self.success_code = success_code
        self.created_records = {'results': []}
        self.error_records = {'inputs': []}
        self.api_calls = 0

    def error_process(self, process_list):
        if len(process_list['inputs']) == 1:
            self.error_records['inputs'].extend(process_list['inputs'])
            return
        
        # Split lists in half
        first_half = process_list['inputs'][:len(process_list['inputs'])//2]
        second_half = process_list['inputs'][len(process_list['inputs'])//2:]

        # Shuffle the order of the inputs. Does not need to be used
        # random.shuffle(first_half)
        # random.shuffle(second_half)

        payloads = [
            {'inputs': first_half},
            {'inputs': second_half}
        ]

        for payload in payloads:
            created_records = batch_create_records(
                object_type=self.object_type,
                payload=payload
            )
            self.api_calls += 1
            if not created_records.ok:
                self.error_process(payload)
            else:
                self.created_records['results'].extend(created_records.json()['results'])


def unit_create_record(object_type, payload):
    url = f'{BASE_URL}/{object_type}'
    res = requests.post(
        url=url,
        data=json.dumps(payload),
        headers=headers
    )

def batch_create_records(object_type, payload):
    url = f'{BASE_URL}/{object_type}/batch/create'
    res = requests.post(
        url=url,
        data=json.dumps(payload),
        headers=headers
    )
    return res

def main():
    with open('payloads.json', 'r') as f:
        payloads = json.load(f)
    payload = payloads['duplicate_email']
    success_code = 201
    created_records = batch_create_records(
        object_type='contacts',
        payload=payload
    )
    if not created_records.ok:
        error_processor = HubspotBatchErrorHandler(
            object_type='contacts',
            payload=payload,
            response=created_records,
            success_code=success_code
        )
        error_processor.error_process(error_processor.payload)
        created_records = error_processor.created_records
        error_records = error_processor.error_records
        print(error_processor.api_calls)
    else:
        created_records = created_records.json()['results']
        error_records = None

    print(created_records)
    print(error_records)

if __name__ == '__main__':
    main()
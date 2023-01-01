import pandas as pd
import json
import time
import os
import requests

from tqdm import tqdm

params = json.load(open("config/extractor_config.json"))

target = params["target"]

dr = pd.date_range(params['start-date'], params['end-date'],
                   freq='MS').strftime('%Y-%m').tolist()

def check(code, cmd):
    if code != 0:
        notify(f'ERROR: {cmd} returned code {code}')

def extract(date):
    start = time.time()
    notify(f'Extracting files from {date} to folder \'extract\'')
    cmd = f'mkdir -p {target}/extract/{date}'
    check(os.system(cmd), cmd)

    cmd = f'python src/reddit.py {date} --task=extract --ignore_keys=True --reddit_input data/reddit --reddit_output {target} > logs/extract.{date}.stat.tsv.log 2>&1'
    check(os.system(cmd), cmd)

    cmd = f'gzip -f {target}/extract/{date}/rc*.tsv'
    check(os.system(cmd), cmd)

    cmd = f'gzip -f {target}/extract/{date}/rs*.tsv'
    check(os.system(cmd), cmd)

    notify(f'Finished {date} in {time.time() - start}s')

def notify(message):
    results = {}
    results['message'] = message
    #requests.post('https://maker.ifttt.com/trigger/script/json/with/key/[IFTTT-API-KEY]', data=results)

now = time.time()
notify('Create extracts by month')

for date in tqdm(dr):
    extract(date)

notify(f'finished in {time.time() - now}')
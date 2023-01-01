import pandas as pd
import json
import time
import os
import requests

from tqdm import tqdm

params = json.load(open("config/extractor_config.json"))

min_depth = params["min-depth"]
max_depth = params["max-depth"]
min_score = params["min-score"]
title = params["title"]
leaves_only = params["leaves-only"]
target = params["target"]
subreddits_whitelist = params["subreddits-whitelist"]


dr = pd.date_range(params['start-date'], params['end-date'],
                   freq='MS').strftime('%Y-%m').tolist()

def check(code, cmd):
    if code != 0:
        notify(f'ERROR: {cmd} returned code {code}')

def conv(date):
    print(date)
    start = time.time()
    notify(f'Extracting conversations from {date} to folder \'conv\'')
    cmd = f'mkdir -p {target}/conv'
    check(os.system(cmd), cmd)

    cmd = f'python src/reddit.py {date} --task=conv --ignore_keys=True --parallel=True {subreddits_whitelist} --reddit_input data/reddit --reddit_output {target} --clean False --min_score {min_score} --min_depth {min_depth} --max_depth {max_depth} --use_title {title} --leaves_only {leaves_only} > logs/conv.{date}.tsv.gz.log 2>&1'
    check(os.system(cmd), cmd)

    cmd = f'gzip -f {target}/conv/{date}.tsv'
    check(os.system(cmd), cmd)

    notify(f'Finished {date} in {time.time() - start}s')


def notify(message):
    results = {}
    results['message'] = message
    #requests.post('https://maker.ifttt.com/trigger/script/json/with/key/[IFTTT-API-KEY]', data=results)

now = time.time()
notify('Create convs by month')

for date in tqdm(dr):
    conv(date)
notify(f'finished in {time.time() - now}')

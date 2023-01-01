import pandas as pd
import json
import time
import os
import requests

params = json.load(open("config/extractor_config.json"))

target = params["target"]

def check(code, cmd):
    if code != 0:
        notify(f'ERROR: {cmd} returned code {code}')

def concat():
    start = time.time()
    notify(f'Building converstations')

    cmd = f'gzcat {target}/conv/*.tsv.gz | cut -f 2-3 > {target}/conv.tsv'
    check(os.system(cmd), cmd)

    notify(f'Finished conv.tsv.gz in {time.time() - start}s')

def notify(message):
    results = {}
    results['message'] = message
    #requests.post('https://maker.ifttt.com/trigger/script_update/json/with/key/[IFTTT-API-KEY]', data=results)

now = time.time()
notify('Concat Datasets')

concat()

notify(f'finished in {time.time() - now}')

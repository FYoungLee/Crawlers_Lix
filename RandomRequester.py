'''
This module is for getting random User-Agent and random IP
'''

import json
import requests
import random
from bs4 import BeautifulSoup

# get User-Agent from local json file.
def get_ua():
    try:
        with open('UserAgent.json', 'r') as f:
            return json.loads(f.read())
    except FileNotFoundError:
        print('File "UserAgent.json" can not be found, Please make sure putting them together.')
        return None

# download ip addresses from the site 'haoip.cc'
def dl_ips():
    req = requests.get('http://haoip.cc/tiqu.htm')
    soup = BeautifulSoup(req.text, 'lxml')
    ips = soup.find('div', class_='col-xs-12').text.replace('\n', '').split(' ')
    return list(filter(lambda x: x.strip(), ips))


# get ip address from local json file.
def get_ips():
    with open('IPs.json', 'r') as f:
        return json.loads(f.read())

# save ip address to local json file.
def save_ips(ips):
    with open('IPs.json', 'a') as f:
        f.write(json.dumps(ips))

# check ip that download from internet.
def goodip(num):
    ips = dl_ips()
    uas = get_ua()

    def check_ip(ippak):
        gips = []
        for each in ippak:
            ua = random.choice(uas)
            try:
                req = requests.get('http://wap.189.cn/', headers={'User-Agent': ua}, proxies={'http': each}, timeout=3)
                if req.ok:
                    gips.append(each)
                print(each, req.ok)
            except BaseException:
                print(each, 'is bad ip.')
        return gips

    goodips = check_ip(ips)
    while len(goodips) < num:
        print('ip : ', len(goodips))
        ips = dl_ips()
        goodips.extend(check_ip(ips))

    save_ips(goodips)

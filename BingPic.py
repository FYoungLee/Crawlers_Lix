# BaiduPic.py
# Created: 25th Oct 2016
# Author: Fyoung Lix
# Version: v0.000000001

"""
    Pictures downloading from the Bing
"""

from urllib.request import urlopen, urlretrieve, Request
from urllib.parse import urlencode
from bs4 import BeautifulSoup as bsoup
import os

def save_img(imgs, path):
    for each in imgs:
        name = each.split('/')[-1]
        print('downloading', name, '......')
        try:
            urlretrieve(each, path+'/'+name)
        except:
            print('failed download', name, ' trying next one.')

def scrap_imgs(url):
    print('go to page: ', url)
    req = Request(url,
                  headers={'User-Agent':
                               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 '
                               '(KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36 OPR/39.0.2256.48'})
    bsobj = bsoup(urlopen(req), 'lxml')
    imgs = []
    for each in bsobj.find('div', {'id':'canvas'}).find('div', {'class':'content'}).findAll('a', {'class':'thumb'}):
        imgs.append(each['href'])
    return imgs, bsobj.find('div', {'id':'canvas'}).find('div', {'class':'content'}).find('a', {'class':'nav_page_next'})['href']

def start():
    site = 'http://cn.bing.com'
    keywd = input('what >> ')
    pages = int(input('How many pages you want to download >> '))
    dat = {'q':keywd, 'go':'Search', 'qs':'bs', 'form':'QBIR'}
    path = '/users/mac/downloads/' + keywd
    url = site + '/images/search?' + urlencode(dat)     
    try:
        os.mkdir(path)
    except:
        pass
    os.chdir(path)
    for each in range(pages):
        imgs, url = scrap_imgs(url)
        url = site + url
        save_img(imgs, path)

if __name__ == '__main__':
    start()

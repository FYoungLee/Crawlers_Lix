# BaiduPic.py
# Created: 15th Nov 2016
# Author: Fyoung Lix
# Version: v0.000000001

"""
    Pictures downloading from the Baidu
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from bs4 import BeautifulSoup
from urllib.request import urlretrieve
import os, threading


class BaiduPictureDownloader:
    # initialize the webdriver using PhantomJS
    def __init__(self):
        self.BDdriver = webdriver.PhantomJS('/Applications/PhantomJS/bin/PhantomJS')

    # get keywords from user
    def set_keywd(self):
        return input('What kind picture are you looking for >> ')

    # start crawling.
    def get_start(self, keyword):
        pages = input('How many pages >> ')
        self.BDdriver.get('http://image.baidu.com')
        self.BDdriver.find_element_by_xpath('//*[@id="kw"]').send_keys(keyword + Keys.RETURN)
        try:
            WebDriverWait(self.BDdriver, 10).until(
                expected_conditions.presence_of_all_elements_located((By.XPATH, '//*[@id="imgid"]/div[1]/ul')))
        except:
            print('Failed on searching')
            return None
        t = 1
        # Scroll down pages until found the content.
        while True:
            try:
                WebDriverWait(self.BDdriver, 0).until(expected_conditions.presence_of_all_elements_located(
                    (By.XPATH, '//*[@id="imgid"]/div[{}]/ul'.format(pages))))
                return
            except:
                self.BDdriver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                print('Scrolling page {} ...'.format(t))
                t += 1

    # Make a BeautifulSoup object from page source and return it.
    def make_soup(self):
        picsoup = BeautifulSoup(self.BDdriver.page_source, 'lxml')
        return [each['data-objurl'] for each in picsoup.findAll('li', {'class': 'imgitem'})]

    # Create the dir depend on user input and keyword
    def create_dir(self, keyword):
        savepath = input('Save to (Return == Current Direction) >> ')
        if savepath == '':
            savepath = os.getcwd()
        tarpath = savepath + '/' + keyword
        try:
            os.mkdir(tarpath)
        except:
            print('Failed on creating path, try again')
            self.create_dir(keyword)
        return tarpath

    # Saving all pictures from the Beautifulsoup Object.
    def save_pic(self, soupobj, savedir):
        n = 1
        for each in soupobj:
            print('Saving picture {}/{}'.format(n, len(soupobj)))
            try:
                urlretrieve(each, savedir + '/' + each.split('/')[-1])
            except:
                print('Failed on saving picture {}'.format(n))
            n += 1

if __name__ == '__main__':
    # create Downloader object
    pics = BaiduPictureDownloader()
    # get Keyword you are looking for.
    kw = pics.set_keywd()
    # start crawling
    pics.get_start(kw)
    # get a BeautifulSoup Object
    psoup = pics.make_soup()
    # Creating Dir
    spath = pics.create_dir(kw)
    # saving all pictures
    pics.save_pic(psoup, spath)
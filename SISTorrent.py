"""
    Multiply Threads Crawler for downloading torrents from SexInSex fourm
    Author: Fyound Lix
    Create: 11/05/2016
    Version: 1.0
"""

from http import cookiejar
from urllib.request import HTTPCookieProcessor, build_opener, Request, urlretrieve
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import re
import os
import time
import socket
import threading


# function for making BeautifulSoup Object
def get_bsobj(which_site):
    req = Request(which_site)
    req.add_header('User-Agent', UserAgent)
    opener = build_opener(HTTPCookieProcessor(cookie))
    result = opener.open(req)
    return BeautifulSoup(result, 'html.parser')


# function for making cookies
def get_cookies_from_sis(usernm, passwd):
    file = 'siscookies.txt'
    cooker = cookiejar.MozillaCookieJar(file)
    handler = HTTPCookieProcessor(cooker)
    opener = build_opener(handler)
    postdata = urlencode(
        {'62838ebfea47071969cead9d87a2f1f7': usernm, 'c95b1308bda0a3589f68f75d23b15938': passwd})
    opener.open('http://38.103.161.156/bbs/index.php', postdata.encode())
    cooker.save(ignore_discard=True, ignore_expires=True)


# function for downloading all pictures, torrents and movie information.
def dl_content_from_topic(topic_list, path_sym):
    for each_page in topic_list:
        # get target path
        tar_path = download_path + path_sym + each_page[0]
        tar_page = basesite + each_page[2]
        try:
            os.mkdir(tar_path)
        except FileExistsError:
            pass
        tar_path += path_sym + each_page[1].replace('/', '-').replace(':', '').replace('<', '').replace('>', '') + path_sym
        print('({}) Downloading {} to {}'.
              format(time.ctime()[11:], tar_page, tar_path))
        try:
            os.mkdir(tar_path)
        except FileExistsError:
            continue
        except OSError as err:
            print('Making Dir err, Please check it.', err)
            continue
        pagesoup = get_bsobj(tar_page)
        # get movie information
        ct1 = pagesoup.find('div', {'class': 't_msgfont'})
        # get torrents address
        ct2 = pagesoup.find_all('a', {'href': re.compile(r'attachment')})
        if ct1 is None or ct2 is None:
            continue
        # download torrents
        for each_attach in ct2:
            try:
                urlretrieve(basesite + each_attach['href'], filename=tar_path + each_attach.text)
            except:
                print('Bad torrent file, trying next one.')
                continue
        # download pictures
        for each_pic in ct1.find_all('img', {'src': re.compile(r'jpg|png')})[:3]:
            pic_url = each_pic['src']
            file_name = pic_url[pic_url.rfind('/') + 1:]
            try:
                urlretrieve(pic_url, filename=tar_path + file_name)
            except BaseException:
                print('Bad picture, trying next one.')
                continue
        # write movie information to local file
        with open(tar_path + 'info.txt', 'w') as f:
            try:
                f.write(ct1.text)
            except:
                print('Writing infomation failed')


# get all topic pages from the given sub fourm
def get_all_topic(onsite, start_page, end_page, path_sym):
    # making pages list from given range
    all_pages = [onsite + '{}.html'.format(each) for each in range(start_page, end_page + 1)]
    for each in all_pages:
        # topic list
        all_topic = []
        print('Downloading all pages in {}'.format(each))
        # make soup object
        sisoup = get_bsobj(each)
        raw_topics = sisoup.findAll('tbody')
        for e in raw_topics:
            try:
                # requrie the movie type
                topic_type = e.find('th').find('em').find('a').text
            except AttributeError:
                continue
            # filter the non-moive topic
            if topic_type == '版务':
                continue
            name = e.span.a.text
            url = e.find('a')['href']
            all_topic.append((topic_type, name, url))
        # start download all topic in this page
        dl_content_from_topic(all_topic, path_sym)

if __name__ == '__main__':
    # set default timeout when downloading
    socket.setdefaulttimeout(120)
    # set the base infomation of website
    UserAgent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.59 Safari/537.36 OPR/41.0.2353.46'
    basesite = 'http://38.103.161.156/forum/'
    # choose working system
    print('1 Windows')
    print('2 Mac OS or Linux')
    cmd = input('Your OS >> ')
    path_symbol = '/'
    if cmd == '1':
        path_symbol = '\\'
    elif cmd == '2':
        pass
    else:
        print('Bad input, bye.')
        exit()
    # config default saving path
    savepath = input('Saving Path (Default location[{}])>> '.format(os.getcwd()))
    if savepath == '':
        savepath = os.getcwd()
    download_path = savepath + path_symbol + 'SIS'
    try:
        print('Creating Dir : {}'.format(download_path))
        os.mkdir(download_path)
    except:
        print('Dir already exists.')
    print('Download path = {}'.format(download_path))
    # choose which sub-fourm to download
    print('1 Asia Uncensored Authorship Seed | 亚洲无码原创区')
    print('2 Asia Censored Authorship Seed | 亚洲有码原创区')
    print('3 Western Uncensored Authorship Seed | 欧美无码原创区')
    print('4 Anime Authorship Seed | 成人游戏动漫原创区')
    cmd = input('>> ')
    site = ''
    if cmd == '1':
        site = basesite + 'forum-143-'
        download_path += path_symbol + '亚洲无码原创区'
    elif cmd == '2':
        site = basesite + 'forum-230-'
        download_path += path_symbol + '亚洲有码原创区'
    elif cmd == '3':
        site = basesite + 'forum-229-'
        download_path += path_symbol + '欧美无码原创区'
    elif cmd == '4':
        site = basesite + 'forum-231-'
        download_path += path_symbol + '成人游戏动漫原创区'
    else:
        print('Bad input, bye.')
        exit()
    try:
        print('Creating Dir : {}'.format(download_path))
        os.mkdir(download_path)
    except:
        print('Dir already exists.')
    # get login name and password to make a cookie file.
    loginnm = input('Login Name >> ')
    loginpw = input('Login Password >> ')
    get_cookies_from_sis(loginnm, loginpw)
    # load cookie infomation from the file
    cookie_file = 'siscookies.txt'
    cookie = cookiejar.MozillaCookieJar()
    cookie.load(cookie_file, ignore_expires=True, ignore_discard=True)
    os.remove(cookie_file)

    # set pages and threads etc.
    pages = input('How many pages download >> ')
    threads = input('How many downloading thread >> ')
    divpages = int(int(pages)/int(threads))
    startpage = 1

    # start crawling
    dlthreads = []
    for eachthread in range(int(threads)):
        endpage = startpage + divpages - 1
        t = threading.Thread(target=get_all_topic, args=(site, startpage, endpage, path_symbol))
        t.setDaemon(True)
        t.start()
        dlthreads.append(t)
        print('Thread {} start crawling from {} to {}'.format(eachthread, startpage, endpage))
        startpage += divpages

    for each in dlthreads:
        each.join()

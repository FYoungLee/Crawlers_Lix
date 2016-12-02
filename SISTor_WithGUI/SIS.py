"""
    Multiply Threads Crawler for downloading torrents from SexInSex fourm
    Author: Fyound Lix
    Create: 11/05/2016
    Version: 1.0
"""
from bs4 import BeautifulSoup
import re
import os
import datetime
import requests
import random
from PyQt5.QtCore import QThread
from PyQt5.QtCore import pyqtSignal


class SISObj(QThread):
    trigger_text = pyqtSignal(str)
    trigger_progress = pyqtSignal(int)
    trigger_sent_all_topics_quantity = pyqtSignal(int)
    trigger_done = pyqtSignal(int)

    def __init__(self, tn, url='', forum='', username='', password='', save_dir='', start=1, end=2, pics=0, parent=None):
        super(SISObj, self).__init__(parent)
        self.name = tn + 1
        self.sis_url = url
        self.forum = forum
        self.slash = self.get_os_slash()
        self.headers = self.get_headers()
        self.cookies = None
        if username is not '' and password is not '':
            self.cookies = self.get_sis_cookies(username, password)
        self.save_dir = save_dir + self.get_sub_forum_name(forum)
        try:
            os.mkdir(self.save_dir)
        except:
            pass
        self.start_page = start
        self.end_page = end
        self.pics = pics

    def run(self):
        self.get_start()

    def get_os_slash(self):
        if os.name == 'posix':
            return '/'
        elif os.name == 'nt':
            return '\\'
        else:
            print('Unkown Operate System.(未知操作系统)')
            self.trigger_text.emit('Unkown Operate System.(未知操作系统)')

    def get_headers(self):
        UserAgents = ['Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9a2pre) Gecko/20061231 Minefield/3.0a2pre',
                      'Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.8.1.12) Gecko/20080203 SUSE/2.0.0.12-6.1 Firefox/2.0.0.12',
                      'Mozilla/5.0 (X11; U; FreeBSD i386; ru-RU; rv:1.9.1.3) Gecko/20090913 Firefox/3.5.3',
                      'Mozilla/5.0 (X11; U; Linux i686; fr; rv:1.9.0.1) Gecko/2008070206 Firefox/2.0.0.8',
                      'Mozilla/4.0 (compatible; MSIE 5.0; Linux 2.4.20-686 i686) Opera 6.02  [en]']
        return random.choice(UserAgents)

    def get_sis_cookies(self, username, password):
        login_data = {'action': 'login',
                      'loginsubmit': 'true',
                      '62838ebfea47071969cead9d87a2f1f7': username,
                      'c95b1308bda0a3589f68f75d23b15938': password}
        ck = requests.post(self.sis_url + 'logging.php', data=login_data).cookies
        if len(ck.values()[0]) > 10:
            return ck
        else:
            return None

    def get_sub_forum_name(self, sub_addr):
        if sub_addr == 'forum-143-':
            return 'Asia Uncensored Authorship Seed 亚洲无码原创区'
        elif sub_addr == 'forum-230-':
            return 'Asia Censored Authorship Seed 亚洲有码原创区'
        elif sub_addr == 'forum-229-':
            return 'Western Uncensored Authorship Seed 欧美无码原创区'
        elif sub_addr == 'forum-231-':
            return 'Anime Authorship Seed 成人游戏动漫原创区'
        elif sub_addr == 'forum-25-':
            return 'Asia Uncensored Section | 亚洲无码转帖区 '
        elif sub_addr == 'forum-58-':
            return 'Asia Censored Section | 亚洲有码转帖区'
        elif sub_addr == 'forum-77-':
            return 'Western Uncensored | 欧美无码转帖区'
        elif sub_addr == 'forum-27-':
            return 'Anime Fans Castle | 成人游戏动漫转帖区'

    # def set_sis_url(self, url):
    #     self.sis_url = url

    def make_soup(self, url):
        response = requests.get(url, headers=self.headers, cookies=self.cookies, timeout=10)
        return BeautifulSoup(response.content, 'lxml')

    # get all topic pages from the given sub fourm
    def get_start(self):
        # making pages list from given range
        url = self.sis_url + self.forum
        all_pages = [url + '{}.html'.format(each) for each in range(self.start_page, self.end_page + 1)]
        all_topics = []
        self.trigger_text.emit('({}) Collecting all topics from pages (准备开始搜集所有页面主题信息)'.format(self.name))
        for each_page in all_pages:
            # topic list
            try :
                topics = self.extract_info_from_page(each_page)
                all_topics.extend(topics)
            except:
                self.trigger_text.emit('({}) Get {} failed (页面获取错误)'.format(self.name, each_page))
        # start download all topic in this page
        self.trigger_sent_all_topics_quantity.emit(len(all_topics))
        self.trigger_text.emit('Downloading Start! 开始下载')
        self.download_content_from_topic(all_topics)

    def extract_info_from_page(self, page):
        ret = []
        print('Downloading all topics in {}'.format(page))
        self.trigger_text.emit('Downloading all topics address from page {} (中获取主题)'.format(page))
        # make soup object
        sisoup = self.make_soup(page)
        raw_info = sisoup.findAll('tbody')
        for e in raw_info:
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
            ret.append((topic_type, name, url))
        return ret

    def download_content_from_topic(self, topics):
        for each_page in topics:
            # get target path
            tar_path = self.save_dir + self.slash + each_page[0]
            tar_page = self.sis_url + each_page[2]
            try:
                os.mkdir(tar_path)
            except FileExistsError:
                pass
            clean_topic_name = each_page[1].replace('/', '-').replace(':', '').replace('<', '').replace('>', '')
            tar_path += self.slash + clean_topic_name + self.slash
            try:
                print('({}) {}'.
                      format(datetime.datetime.now().strftime('%H:%M:%S'), tar_path.split(self.slash)[-2]))
                self.trigger_text.emit('({}) Downloading {}'.format(self.name, tar_path.split(self.slash)[-2]))
            except BaseException as err:
                print('\n\t\t{}\n'.format(err))
            try:
                os.mkdir(tar_path)
            except FileExistsError:
                print('\t\t\t{} already downloaded (已经下载过了).'.format(clean_topic_name))
                self.trigger_text.emit('({}) {} already downloaded (已经下载过了).'.format(self.name, clean_topic_name))
                self.trigger_progress.emit(1)
                continue
            except OSError as err:
                print('Making Dir err, Please check it.', err)
                self.trigger_text.emit('({}) Making Dir err, Please check it.\n{}'.format(self.name, err))
                continue
            pagesoup = self.make_soup(tar_page)
            # get movie information
            page_info = pagesoup.find('td', {'class': 'postcontent'})
            # get torrents address and download
            try:
                self.save_torrents(page_info, tar_path)
            except BaseException as err:
                print(err)
                self.trigger_text.emit(err)
                continue
            # write movie information to local file
            self.save_info_text(page_info, tar_path)
            # download pictures
            self.save_pictures(page_info, tar_path)
            self.trigger_progress.emit(1)
        self.trigger_done.emit(1)

    def save_torrents(self, pagesoup, tar_path):
        page_tors = pagesoup.find_all('a', {'href': re.compile(r'attachment')})
        if page_tors is None:
            raise BaseException('Broken page (页面错误)')
        # download torrents
        for each_attach in page_tors:
            try:
                print('\t\t\t{}'.format(each_attach.text))
                filename = tar_path + each_attach.text
                with open(filename, 'wb') as tor:
                    tor.write(requests.get(self.sis_url + each_attach['href']).content)
            except BaseException as err:
                print('\t\t\t{}'.format(err))
                continue

    def save_info_text(self, page_info, tar_path):
        text = page_info.find('div', {'class': 't_msgfont'}).text
        with open(tar_path + 'info.txt', 'w') as f:
            try:
                f.write(text)
            except:
                print('\t\t\tInformation download failed.')
                return
        print('\t\t\tinfo.txt')

    def save_pictures(self, page_info, tar_path):
        for each_pic in page_info.find_all('img', {'src': re.compile(r'jpg|png')})[:self.pics]:
            pic_url = each_pic['src']
            filename = pic_url[pic_url.rfind('/') + 1:]
            try:
                print('\t\t\t{}'.format(filename))
                filename_with_path = tar_path + filename
                with open(filename_with_path, 'wb') as pic:
                    pic.write(requests.get(pic_url, timeout=30).content)
            except BaseException:
                print('\t\t\tBroken picture, trying next one.')
                continue
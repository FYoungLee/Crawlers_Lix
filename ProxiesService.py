'''
This module is for getting random User-Agent and random Proxy
'''

import json
import requests
import socket
import logging
import random
import time
from bs4 import BeautifulSoup
from threading import Thread, Lock
from queue import Queue
import re

PORT = 8888


class Requester:
    User_Agent_file = 'UserAgent.json'
    # Proxies_file = 'Proxies.json'

    def __init__(self, tar_url):
        self.user_agents = self.get_ua()
        self.good_proxies = []
        self.raw_proxies = Queue()
        self.socket = socket.socket()
        self.Url_verify = tar_url
        self.my_ip = self.get_my_ip()

    def get_my_ip(self):
        resp = requests.get(self.Url_verify, timeout=10)
        return re.search(r'(\d+\.\d+\.\d+\.\d+)', resp.text).group()

    @classmethod
    def get_ua(cls):
        try:
            with open(cls.User_Agent_file, 'r') as f:
                return json.loads(f.read())
        except FileNotFoundError:
            logging.critical('File "UserAgent.json" can not be found.')
            return

    # @classmethod
    # def load_local_proxies(cls):
    #     with open(cls.Proxies_file, 'r') as f:
    #         return json.loads(f.read())
    #
    # @classmethod
    # def save_proxies_to_local(cls, proxies):
    #     with open(cls.Proxies_file, 'a') as f:
    #         f.write(json.dumps(proxies))

    def random_user_agent(self):
        return {'User-Agent': random.choice(self.user_agents)}

    def grab_proxies_from_youdaili(self):
        url_pattern = 'http://www.youdaili.net/Daili/{}/list_{}.html'

        def grab_all(content):
            proxies = re.findall(r'(\d+(\.\d+){3}:\d+)', content.decode('utf8', errors='ignore'))
            return [x[0] for x in proxies]

        for page in ['http', 'guonei', 'guowai']:
            for index in range(1, 35):
                url = url_pattern.format(page, index)
                try:
                    content = self.random_req(url)
                    if not content:
                        continue
                    raw_soup = BeautifulSoup(content, 'lxml')
                    allpages = raw_soup.find('div', {'class': 'chunlist'}).find_all('li')
                    for each in allpages:
                        urls = [each.find('a')['href']]
                        for _p in range(2, 6):
                            urls.append(urls[0].replace('.html', '_{}.html'.format(_p)))
                        for _u in urls:
                            logger('loading {}'.format(_u))
                            try:
                                content = self.random_req(_u)
                                if not content:
                                    continue
                                proxies = grab_all(content)
                                for proxy in proxies:
                                    self.raw_proxies.put(proxy)
                            except Exception as err:
                                logger('loading {} error {}'.format(_u, err))
                        logger('{} good proxies in stock.'.format(len(self.good_proxies)))
                        if not self.good_proxies:
                            time.sleep(5)
                    if not self.good_proxies:
                        time.sleep(10)
                except Exception as err:
                    logger('{} {}'.format(url, err))

    def random_req(self, url):
        header = self.random_user_agent()
        proxy = None
        if self.good_proxies:
            proxy = random.choice(self.good_proxies)
        try:
            req = requests.get(url, headers=header, timeout=10, proxies=proxy)
            return req.content
        except Exception:
            req = requests.get(url, headers=header, timeout=10)
            return req.content

    def verify_proxies_thread(self, howmany):
        locker = Lock()

        def fecth():
            while not self.raw_proxies.empty():
                proxy = {'http': self.raw_proxies.get()}
                try:
                    resp = requests.get(self.Url_verify, proxies=proxy, timeout=10)
                    if proxy['http'].split(':')[0] in resp.text and self.my_ip not in resp.text:
                        with locker:
                            self.good_proxies.append(proxy)
                except Exception as err:
                    pass

        for th in range(howmany):
            Thread(target=fecth).start()

    def proxies_service(self):
        Thread(target=self.grab_proxies_from_youdaili).start()
        while self.raw_proxies.empty():
            time.sleep(1)
        self.verify_proxies_thread(20)
        while True:
            for each in self.good_proxies:
                try:
                    resp = requests.get(self.Url_verify, proxies=each, timeout=10)
                    if not resp.ok:
                        self.good_proxies.remove(each)
                    time.sleep(1/len(self.good_proxies))
                except Exception:
                    self.good_proxies.remove(each)

    def proxies_server(self):
        self.socket.bind(('', PORT))
        self.socket.listen(5)
        while True:
            client, address = self.socket.accept()
            logger('Requests from {}:{}'.format(address[0], address[1]))
            data = client.recv(1024)
            if b'proxy' in data:
                try:
                    resp = json.dumps(self.good_proxies)
                    client.send(resp.encode())
                except Exception as err:
                    client.send(b'error')
            elif b'bad' in data:
                addr = data.decode().split(' ')[-1]
                self.good_proxies.remove({'http': addr})
            client.close()
        # inputs = [self.socket, ]
        # outputs = []
        # msg = {}
        # while True:
        #     reader, writer, _ = select.select(inputs, outputs, [])
        #     for each in reader:
        #         if each is self.socket:
        #             client, address = each.accept()
        #             client.setblocking(0)
        #             inputs.append(client)
        #         else:
        #             data = each.recv(1024)
        #             if each not in outputs and data:
        #                 outputs.append(each)
        #                 msg[each.fileno()] = data
        #             else:
        #                 try:
        #                     inputs.remove(each)
        #                     outputs.remove(each)
        #                 except (ValueError, IndexError, KeyError):
        #                     continue
        #                 finally:
        #                     each.close()
        #     for each in writer:
        #         key = each.fileno()
        #         try:
        #             data = msg[key]
        #             if b'proxy' in data:
        #                 proxy = random.choice(self.good_proxies)
        #                 each.send(str(proxy).encode())
        #             elif b'user agent' in data:
        #                 us = self.random_user_agent()
        #                 each.send(us.encode())
        #             msg.pop(key)
        #             outputs.remove(each)
        #             inputs.remove(each)
        #         except (ValueError, IndexError, KeyError):
        #             pass
        #         finally:
        #             each.close()

    def starts(self):
        logger('Starting proxies service at {}:{}'.format(socket.gethostbyname(socket.gethostname()), PORT))
        Thread(target=self.proxies_service).start()
        self.proxies_server()


def logger(msg):
    print('[{}] {}'.format(time.ctime(), msg))
    
if __name__ == '__main__':
    pool = []
    srv = Requester('http://httpbin.org/ip')
    srv.starts()
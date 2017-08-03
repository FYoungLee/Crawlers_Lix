from selenium import webdriver, common
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.proxy import ProxyType
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from threading import Thread, Lock
from queue import Queue
import time
import socket
import random
import json

CITIES = {'成都': 'CTU', '普吉岛': 'HKT', '甲米岛': 'KBV', '巴厘岛': 'DPS', '孟买': 'DMK'}


class GetFlight(Thread):
    # initialize the webdriver using PhantomJS
    def __init__(self):
        super().__init__()
        self.browser = webdriver.PhantomJS('/Applications/PhantomJS/bin/PhantomJS')
        self.proxies_server = ('192.168.0.49', 8888)
        self.proxies_pool = []

    def run(self):
        self.do_job()

    def get_proxy(self):
        so = socket.socket()
        so.settimeout(3)
        so.connect(self.proxies_server)
        so.send(b'proxy')
        try:
            self.proxies_pool.extend(json.loads(so.recv(1024).decode()))
        except socket.timeout:
            return

    def bad_proxy(self, proxy):
        so = socket.socket()
        so.settimeout(3)
        so.connect(self.proxies_server)
        so.send(b'bad ' + proxy.encode())
        so.close()

    def get_round_trip_lionair(self):
        ptn = 'https://search.lionairthai.com/SL/Flight.aspx' \
              '?depCity={}&arrCity={}&depDate=30/04/2017&arrDate=09/05/2017&adult1=1&child1=0&infant1=0&culture=en-GB&df=UK&afid=0&b2b=0&St=fa&DFlight=false&roomcount=1&sid=MgAyADIALgAyADEAMQAuADIAMgAyAC4AMQA2ADIA&t=1CB'


    def get_round_trip_ctrip(self, departure, destination, depart_date, return_date, adult=1, kid=0, baby=0, proxy_addr=None):
        if proxy_addr is not None:
            proxy = webdriver.Proxy()
            proxy.proxy_type = ProxyType.MANUAL
            proxy.http_proxy = proxy_addr
            proxy.add_to_capabilities(webdriver.DesiredCapabilities.PHANTOMJS)
            self.browser.start_session(webdriver.DesiredCapabilities.PHANTOMJS)
        ptn = 'http://m.ctrip.com/html5/flight/matrix-list-intl.html' \
              '?dcity={}&acity={}&trip=2&ddate1={}&ddate2={}&adult={}&child={}&baby={}&' \
              'dfilter=direct%3A%7Csort%3A102%2C2%7Cprice%3A2'
        target_page = ptn.format(CITIES[departure], CITIES[destination], depart_date, return_date, adult, kid, baby)
        try:
            self.browser.get(target_page)
            WebDriverWait(self.browser, 60).until(lambda x: x.find_element_by_css_selector('.flight-filterbar'))
            self.browser.find_element_by_class_name('mf-flight-info1').click()
            time.sleep(2)
            WebDriverWait(self.browser, 60).until(lambda x: x.find_element_by_css_selector('.flight-filterbar'))
            flights_soupobj = BeautifulSoup(self.browser.page_source, 'lxml')
            flights = flights_soupobj.findAll('div', {'class': 'mf-list-contain'})
            flight = flights[0].find('li', {'class': 'mf-main-cabin'})
            company, t_time = [x.strip() for x in flight.find('div', {'class': 'mf-flight-info2'}).text.split('|')]
            depart_time, arrive_time = [x.text for x in flight.find_all('span', {'class': 'mf-list-time'})]
            transit_obj = flight.find('span', {'class': 'mf-flight-turn '})
            if transit_obj:
                transit = [x.strip() for x in transit_obj.text.split(' ') if x.strip()][-1]
            else:
                transit = ''
            depart_flight = FlightINFO(company, depart_time, arrive_time, t_time, transit)
            flight = flights[1].find('li', {'class': 'mf-main-cabin'})
            company, t_time = [x.strip() for x in flight.find('div', {'class': 'mf-flight-info2'}).text.split('|')]
            depart_time, arrive_time = [x.text for x in flight.find_all('span', {'class': 'mf-list-time'})]
            transit_obj = flight.find('span', {'class': 'mf-flight-turn '})
            if transit_obj:
                transit = [x.strip() for x in transit_obj.text.split(' ') if x.strip()][-1]
            else:
                transit = ''
            return_flight = FlightINFO(company, depart_time, arrive_time, t_time, transit)
            price = flight.find('span', {'class': 'mf-flight-price-num'}).text.strip()
            the_trip = RoundTrip(price, dep_city, des_city, depart_date, return_date, depart_flight, return_flight)
            return the_trip
        except common.exceptions.TimeoutException as err:
            if '没有找到符合条件的结果' in self.browser.page_source:
                print('{} 没有找到符合条件的结果'.format(depart_date))
            else:
                print(err)
                raise err
        except Exception:
            raise Exception(target_page)

    # def get_round_trip_igola(self, depart_city, arrive_city, depart_date, return_date):
    #     while True:
    #         # _p = self.get_proxy()
    #         # if not _p:
    #         #     continue
    #         # proxy = webdriver.Proxy()
    #         # proxy.proxy_type = ProxyType.MANUAL
    #         # proxy.http_proxy = _p
    #         # # 将代理设置添加到webdriver.DesiredCapabilities.PHANTOMJS中
    #         # proxy.add_to_capabilities(webdriver.DesiredCapabilities.PHANTOMJS)
    #         # self.browser.start_session(webdriver.DesiredCapabilities.PHANTOMJS)
    #         try:
    #             self.browser.get('https://www.igola.com/flights/ZH/{}-{}_{}*_1*OW*Economy_0*0'
    #                              .format(depart_city, arrive_city, depart_date))
    #             WebDriverWait(self.browser, 10).until(expected_conditions.presence_of_all_elements_located(
    #                     (By.CLASS_NAME, 'wrapper-scroller')))
    #             depart_source = self.browser.page_source
    #             self.browser.get('https://www.igola.com/flights/ZH/{}-{}_{}*_1*OW*Economy_0*0'
    #                              .format(arrive_city, depart_city, return_date))
    #             WebDriverWait(self.browser, 10).until(expected_conditions.presence_of_all_elements_located(
    #                 (By.CLASS_NAME, 'wrapper-scroller')))
    #             return_source = self.browser.page_source
    #             departs = BeautifulSoup(depart_source, 'lxml')
    #             departs = departs.find('div', {'class': 'flight-list'}).findAll('div', {'class': 'flight-row ng-scope'})
    #             returns = BeautifulSoup(return_source, 'lxml')
    #             returns = returns.find('div', {'class': 'flight-list'}).findAll('div', {'class': 'flight-row ng-scope'})
    #             trip = RoundTrip(depart_date, return_date)
    #             for flight in departs:
    #                 price =
    #                 depart_time, arrived_time = [x.text for x in flight.findAll('span', {'class': 'time-text'})]
    #                 company = flight.find('span', {'class': 'alicon'})['title']
    #                 trav_t = flight.find('div', {'class': 'new-alltime'}).text
    #                 transit = flight.find('div', {'class': 'new-stopover'}).text
    #             break
    #         except Exception as err:
    #             print(depart_date, 'Bad request!', err)
    #             time.sleep(self.delay)
    #     while True:
    #         flights = flights_soupobj.find('div', {'id': 'wrapper-scroller'}).find_all('div', {'class': 'flight-baseinfo'})
    #         if len(flights):
    #             ret = []
    #             for flight in flights:
    #                 try:
    #                     price = flight.find('span', {'class': 'price'}).text[1:]
    #                     company_1, company_2 = [x.text for x in flight.find_all('div', {'class': 'airline-name'})]
    #                     depart1, arrived1, depart2, arrived2 = [x.text for x in
    #                                                             flight.find_all('div', {'class': 'flight-detail-time'})]
    #                     travel_time1, travel_time2 = [x.text.strip() for x in
    #                                                   flight.find_all('div', {'class': 'flight-total-time'})]
    #                     try:
    #                         transit_1, transit_2 = [x.text for x in
    #                                                 flight.find_all('span', {'class': 'stop-city stop-city-transfer'})]
    #                     except (AttributeError, ValueError):
    #                         transit_1 = transit_2 = '直飞'
    #                     depart_info = FlightINFO(company_1, depart1, arrived1, travel_time1, transit_1)
    #                     return_info = FlightINFO(company_2, depart2, arrived2, travel_time2, transit_2)
    #                     ret.append(RoundTrip(depart_date, return_date, depart_info, return_info, price))
    #                 except Exception as err:
    #                     print(depart_date, return_date, err)
    #                     continue
    #             if ret:
    #                 print('Best price: ', ret[0])
    #                 print(depart_date, 'finished.')
    #             else:
    #                 print('None')
    #             return ret
    #         else:
    #             print('Page load error, try again...')
    #     time.sleep(self.delay)

    def do_job(self):
        while True:
            if tasks.empty():
                break
            else:
                task = tasks.get()
                try:
                    trip = self.get_round_trip_ctrip(task[0], task[1], task[2], task[3], 6, 1)
                    if trip:
                        print(trip)
                        with locker:
                            results.append(trip)
                    else:
                        print('没有找到符合条件的结果')
                except Exception as err:
                    print('[{}]'.format(time.ctime(), err))
                    tasks.put(task)
                    with open('{}.png'.format(task[0]), 'wb') as f:
                        f.write(self.browser.get_screenshot_as_png())
                time.sleep(random.randint(2, 8))


class FlightINFO:
    def __init__(self, company, depart, arrived, travel_time, transit='无'):
        self.company = company
        self.depart = depart
        self.arrived = arrived
        self.t_time = travel_time
        self.transit = transit

    def __repr__(self):
        return '\t公司: {}\t 起飞: {}\t 抵达: {}\t 时长: {}\t 中转: {}\t'.format(
            self.company, self.depart, self.arrived, self.t_time, self.transit)

    def __str__(self):
        return self.__repr__()


class RoundTrip:
    def __init__(self, price, dep_city, des_city, depart_date, return_date, depart_flight, return_flight):
        self.price = price
        self.dep_city = dep_city
        self.des_city = des_city
        self.depart_date = depart_date
        self.return_date = return_date
        self.depart_flight = depart_flight
        self.return_flight = return_flight

    def __repr__(self):
        ret = '{} - {}  {}[去] {}[回]\t价格: {}'.format(self.dep_city, self.des_city, self.depart_date, self.return_date, self.price)
        ret += '\n去程:\n'
        ret += str(self.depart_flight)
        ret += '\n返程:\n'
        ret += str(self.return_flight)
        ret += '\n' + '='*30 + '\n'
        return ret

    def __lt__(self, other):
        return self.price < other.price

if __name__ == '__main__':
    # create Downloader object
    dep_city = '成都'
    des_city = '普吉岛'
    aday = timedelta(days=1)
    depart_date = datetime.now() + aday*80
    tasks = Queue()
    results = []
    locker = Lock()
    for each in range(40):
        return_date = depart_date + (aday*7)
        tasks.put((dep_city, des_city, depart_date.strftime('%Y-%m-%d'), return_date.strftime('%Y-%m-%d')))
        depart_date += aday
    th = []
    for each in range(1):
        ctrip = GetFlight()
        ctrip.start()
        th.append(ctrip)
    for each in th:
        each.join()
    results.sort()
    print('\n'*5)
    for each in results:
        print(each)

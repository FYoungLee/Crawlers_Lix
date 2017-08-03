from bs4 import BeautifulSoup as bsoup
import sqlite3
import re
from datetime import datetime
import threading
import requests
import time

City_kw_dict = {'bj': '北京', 'sh': '上海', 'gz': '广州', 'sz': '深圳',
                'cd': '成都', 'cq': '重庆', 'nj': '南京', 'hz': '杭州', 'wh': '武汉', 'tj': '天津', 'xian': '西安', 'suzhou': '苏州',
                'cs': '长沙', 'dl': '大连', 'nn': '南宁', 'dg': '东莞', 'jn': '济南', 'xm': '厦门', 'qd': '青岛', 'sjz': '石家庄'}
City_surls = {'bj': 'http://esf.fang.com',
              'sh': 'http://esf.sh.fang.com',
              'gz': 'http://esf.gz.fang.com',
              'sz': 'http://esf.sz.fang.com',
              'tj': 'http://esf.tj.fang.com',
              'nj': 'http://esf.nanjing.fang.com',
              'hz': 'http://esf.hz.fang.com',
              'cd': 'http://esf.cd.fang.com',
              'wh': 'http://esf.wuhan.fang.com',
              'cq': 'http://esf.cq.fang.com',
              'xian': 'http://esf.xian.fang.com',
              'suzhou': 'http://esf.suzhou.fang.com',
              'cs': 'http://esf.cs.fang.com',
              'dl': 'http://esf.dl.fang.com',
              'nn': 'http://esf.nn.fang.com',
              'dg': 'http://esf.dg.fang.com',
              'xm': 'http://esf.xm.fang.com',
              'qd': 'http://esf.qd.fang.com',
              'sjz': 'http://esf.sjz.fang.com',
              'jn': 'http://esf.jn.fang.com'}

sql = sqlite3.connect('CityEstateDB.sql')
sql_tasks = []
latest_date = sql.cursor().execute('SELECT MAX(date) FROM Deals_History').fetchone()[0]

try:
    sql.cursor().execute('CREATE TABLE Deals_History'
                         '(city, district, block, title PRIMARY KEY, price, unit_price, area, date, link)')
except sqlite3.OperationalError:
    pass

# with open('Lianjia_Chengjiao.json') as f:
#     City_Deals = json.loads(f.read())

# async def scrapy_page(session, url, city, area=None, sub_area=None, depth=0):
#     proxy = random.choice(proxies)
#     if proxy:
#         proxy = 'http://' + proxy
#     headers = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9a2pre) Gecko/20061231 Minefield/3.0a2pre'}
#     try:
#         async with session.get(url, proxy=proxy, timeout=10, headers=headers) as resp:
#             pbytes = await resp.read()
#             if resp.status != 200:
#                 raise aiohttp.client_exceptions.ClientError
#             if depth == 0:
#                 _links = cook_pages(pbytes, depth)
#                 for area_name in _links:
#                     if not City_urls.keys() or city not in City_urls.keys():
#                         City_urls[city] = {}
#                     City_urls[city][area_name] = _links[area_name]
#             elif depth == 1:
#                 _links = cook_pages(pbytes, depth)
#                 for area_name in _links:
#                     if isinstance(City_urls[city][area], str):
#                         City_urls[city][area] = {}
#                     City_urls[city][area][area_name] = _links[area_name]
#             elif depth == 2:
#                 max_pg = cook_pages(pbytes, depth)
#                 if max_pg:
#                     for pg in range(1, max_pg+1):
#                         if isinstance(City_urls[city][area][sub_area], str):
#                             City_urls[city][area][sub_area] = []
#                         City_urls[city][area][sub_area].append(url + 'pg{}'.format(pg))
#             elif depth == 3:
#                 City_Deals[city][area][sub_area].extend(cook_pages(pbytes, depth))
#             print('{} {} {} done. [{}] proxy : {}'.format(city, area, sub_area, url, proxy))
#     except (asyncio.TimeoutError, aiohttp.client_exceptions.ClientError) as err:
#         print('[Url]{} [Proxy]{}, [Err Info]{}'.format(url, proxy, err))
#         await scrapy_page(session, url, city, area, sub_area, depth)
#
# async def apply_city():
#     tasks = []
#     conn = aiohttp.TCPConnector(verify_ssl=False)
#     async with aiohttp.ClientSession(connector=conn) as session:
#         for city in City_surls:
#             tasks.append(asyncio.ensure_future(scrapy_page(session, City_surls[city], city)))
#         await asyncio.gather(*tasks)
#
#         tasks.clear()
#         for city in City_surls:
#             city_url = City_surls[city]
#             for area in City_urls[city]:
#                 tasks.append(asyncio.ensure_future(scrapy_page(session,
#                                                                city_url.replace('/chengjiao/', City_urls[city][area]),
#                                                                city,
#                                                                area,
#                                                                depth=1)))
#         await asyncio.gather(*tasks)
#
#         tasks.clear()
#         for city in City_surls:
#             city_url = City_surls[city]
#             for area in City_urls[city]:
#                 for sub_area in City_urls[city][area]:
#                     tasks.append(asyncio.ensure_future(scrapy_page(session,
#                                                                    city_url.replace('/chengjiao/',
#                                                                                     City_urls[city][area][sub_area]),
#                                                                    city,
#                                                                    area,
#                                                                    sub_area,
#                                                                    depth=2)))
#         await asyncio.gather(*tasks)

proxies = [None, ]


class ProxiesThread(threading.Thread):
    headers = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9a2pre) Gecko/20061231 Minefield/3.0a2pre'

    def __init__(self, url, test_url):
        super().__init__()
        self.url = url
        self.test_url = test_url

    def run(self):
        while True:
            for page in range(1, 1700):
                try:
                    req = requests.get(self.url.format(page), headers={'User-Agent': self.headers})
                except requests.exceptions.RequestException as err:
                    print(err)
                    continue
                pobj = bsoup(req.content, 'lxml').findAll('tr')
                for each in pobj[1:]:
                    sp = each.findAll('td')
                    proxy = sp[0].text + ':' + sp[1].text
                    try:
                        if requests.head(self.test_url, proxies={'http': proxy},
                                         headers={'User-Agent': self.headers}, timeout=3).ok:
                            proxies.append(proxy)
                    except requests.exceptions.RequestException:
                        continue


def get_page(url):
    try:
        req = requests.get(url, timeout=5)
        if req.status_code != 200 or req.text is None:
            raise requests.exceptions.RequestException
        return req.text
    except requests.exceptions.RequestException as err:
        time.sleep(1)
        get_page(url)


def scrapy_districts_from_city(city):
    url = City_surls[city]
    page = get_page(url + '/chengjiao/')
    return cook_pages(page, 0)


def scrapy_blocks_from_district(city, district, d_url):
    url = City_surls[city]
    while True:
        try:
            page = get_page(url + d_url)
            blocks = cook_pages(page, 1)
            break
        except TypeError:
            print('{} {} {} BAD Happen, Try again!'.format(city, district, url))
            continue
    for block in blocks:
        scrapy_from_block(city, district, block, blocks[block])


def scrapy_from_block(city, district, block, b_url):
    url = City_surls[city]
    while True:
        try:
            page = get_page(url + b_url)
            max_pg = cook_pages(page, 2)
            scrapy_pages(url + b_url, city, district, block, 1, max_pg + 1)
            break
        except TypeError:
            print('{} {} {} {} BAD Happen, Try again!'.format(city, district, block, url))
            continue


def cook_pages(content, depth):
    psoup = bsoup(content, 'lxml')
    ret = None
    if depth == 0:
        sub_urls = psoup.find('div', {'class': 'qxName'}).find_all('a')
        ret = {x.text: x['href'] for x in sub_urls if
               'http' not in x['href'] and '不限' not in x.text and '周边' not in x.text and '旅游' not in x.text}
    elif depth == 1:
        sub_urls = psoup.find('p', {'id': 'shangQuancontain'}).find_all('a')
        ret = {x.text: x['href'] for x in sub_urls if
               'http' not in x['href'] and '不限' not in x.text and '周边' not in x.text and '旅游' not in x.text}
    elif depth == 2:
        if '共找到 0 套' in psoup.text:
            return 0
        ret = psoup.find('div', {'class': 'fanye'}).find('span').text
        ret = int(re.search(r'\d+', ret).group())
    return ret


def scrapy_pages(url, city, district, block, start, end):
    for pg in range(start, end):
        while True:
            try:
                page = get_page(url + 'i3{}'.format(pg))
                result = cook_page(page, City_surls[city], city, district, block)
                if result == 0:
                    print('{} {} {} {} Updated'.
                          format(City_kw_dict[city], district, block, url + 'i3{}'.format(pg)))
                    return
                print('{} {} {} {} Done'.format(City_kw_dict[city], district, block, url + 'i3{}'.format(pg)))
                break
            except TypeError:
                print('{} {} {} {} BAD Happen, Try again!'.
                      format(City_kw_dict[city], district, block, url + 'i3{}'.format(pg)))


def cook_page(content, url, city, district, block):
    psoup = bsoup(content, 'lxml')
    deals = psoup.find('div', {'class': 'houseList'}).find_all('dl')
    error = 0
    for deal in deals:
        obj = deal.find('dd')
        try:
            price = float(obj.find('span', {'class': 'price'}).text) * 10000
        except AttributeError:
            continue
        link = obj.find('p', {'class': 'title'}).find('a')['href']
        link = url + link
        title = obj.find('p', {'class': 'title'}).find('a').text
        area = float(re.search(r'\d+(\.\d+)?平米', title).group().replace('平米', ''))
        if area:
            unit_price = price / area
            deal_date = obj.find('p', {'class': 'time'}).text
            title = title + ' ' + deal_date
            deal_date = deal_date.split('-')
            deal_date_ts = datetime(year=int(deal_date[0]), month=int(deal_date[1]), day=int(deal_date[2])).timestamp()
            if deal_date_ts < latest_date:
                return 0
            sql_tasks.append((City_kw_dict[city], district, block, title, price, unit_price, area, deal_date_ts, link))
            # age = int(re.search(r'', obj.find('div', {'class': 'positionInfo'}).text).group())
            # deal_info = obj.find('span', {'class': 'dealCycleTxt'}).find_all('span')
            # asking_price = int(re.search(r'\d+', deal_info[0].text)) * 10000
            # period = int(re.search(r'\d+', deal_info[1].text))
            # try:
            #     sql.cursor().execute('INSERT INTO Deals_History VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)',
            #                          (City_kw_dict[city], district, block, title, price, unit_price, area, deal_date_ts, link))
            #     sql.commit()
            # except sqlite3.IntegrityError as err:
            #     error += 1
            #     if error == len(deals):
            #         return 0


if __name__ == '__main__':
    # test_url = 'http://m.sh.lianjia.com/'
    # for each in ('http://www.kuaidaili.com/free/inha/{}/', 'http://www.kuaidaili.com/free/intr/{}/'):
    #     p = ProxiesThread(each, test_url)
    #     p.start()
    # for city in City_surls:
    #     th = threading.Thread(target=scrapy_districts_from_city, args=(city))
    #     th.start()
    # targets = {'bj': '北京'}
    for tar_city in City_kw_dict:
        districts = scrapy_districts_from_city(tar_city)
        for district in districts:
            print('{} is going to load...'.format(district))
            th = threading.Thread(target=scrapy_blocks_from_district, args=(tar_city, district, districts[district]))
            th.start()
        count = 0
        while True:
            if sql_tasks:
                count = 0
                job = sql_tasks.pop()
                try:
                    sql.cursor().execute('INSERT INTO Deals_History VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)', (*job,))
                    print('{} inserted.'.format(job))
                    sql.commit()
                except sqlite3.IntegrityError as err:
                    continue
                    # loop = asyncio.get_event_loop()
                    # loop.run_until_complete(apply_city())

                    # json_str = json.dumps(City_urls)
                    # with open('City_urls.json', 'w') as f:
                    #     f.write(json_str)
            else:
                time.sleep(6)
                count += 1
                if count > 10:
                    break
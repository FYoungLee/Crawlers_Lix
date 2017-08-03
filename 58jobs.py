import re
import sqlite3
import threading
import time

import requests
from bs4 import BeautifulSoup as bsoup

City_kw_dict = {'bj': '北京', 'sh': '上海', 'gz': '广州', 'sz': '深圳',
                'cd': '成都', 'cq': '重庆', 'nj': '南京', 'hz': '杭州', 'wh': '武汉', 'tj': '天津', 'xian': '西安', 'suzhou': '苏州',
                'cs': '长沙', 'dl': '大连', 'nn': '南宁', 'dg': '东莞', 'jn': '济南', 'xm': '厦门', 'qd': '青岛', 'sjz': '石家庄'}

url = 'http://{}.58.com/job/'

sql = sqlite3.connect('58Jobs.sql')
sql_tasks = []
# latest_date = sql.cursor().execute('SELECT MAX(date) FROM Deals_History').fetchone()[0]

try:
    sql.cursor().execute('CREATE TABLE Jobs(city, class, subclass, title, min_salary, max_salary, edu, exp, company)')
except sqlite3.OperationalError:
    pass

proxies = []
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
        req = requests.get(url, timeout=30)
        if req.status_code != 200 or req.text is None:
            raise requests.exceptions.RequestException
        return req.text
    except requests.exceptions.RequestException as err:
        print(url, err)
        time.sleep(1)
        get_page(url)


def scrapy_categories_from_city(city):
    page = get_page(url.format(city))
    return cook_categories(page)


def cook_categories(content):
    psoup = bsoup(content, 'lxml')
    urls = psoup.find('div', {'id': 'filterJob'}).find_all('li')
    urls = {x.text: x.find('a')['href'] for x in urls if '全部' not in x.text and '招聘会' not in x.text}
    return urls


def scrapy_pages(url, city, cate):
    # for pg in range(1, 101):
    next_link = url
    print('Loading ', next_link)
    while True:
        try:
            page = get_page(next_link)
            next_link = cook_page(page, city, cate)
            if next_link:
                print('Loading ', next_link)
            else:
                print(url, 'Skipped >>>')
                return
            break
        except TypeError as err:
            print(url, err)


def cook_page(content, city, cate):
    psoup = bsoup(content, 'lxml')
    try:
        jobs = psoup.find('ul', {'id': 'list_con'}).find_all('li')
    except AttributeError:
        return False
    inserted = 0
    for job in jobs:
        title = job.find('div', {'class': 'job_name clearfix'}).find('span', {'class': 'name'}).text
        salary = job.find('p', {'class': 'job_salary'})
        salaries = re.findall(r'\d+', salary.text)
        subclass, edu, exp = job.find('p', {'class': 'job_require'}).find_all('span')
        com_name = job.find('div', {'class': 'comp_name'}).text
        if len(salaries) == 0:
            salaries = [0, 0]
        elif len(salaries) == 1:
            salaries = [salaries[0], salaries[0]]
        _j = (City_kw_dict[city], cate, subclass.text, title, int(salaries[0]), int(salaries[1]), edu.text, exp.text, com_name)
        check = sql.cursor().execute('SELECT * FROM Jobs WHERE city=? AND title=? AND company=?', (_j[0], _j[3], _j[-1])).fetchone()
        if check:
            inserted += 1
            if inserted > 20:
                return False
        else:
            sql.cursor().execute('INSERT INTO Jobs VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)', (*_j,))
            sql.commit()
            print('{} inserted.'.format(_j))
    try:
        return psoup.find('div', {'class': 'pagesout'}).find('a', {'class': 'next'})['href']
    except (KeyError, AttributeError, TypeError):
        return False


if __name__ == '__main__':
    # test_url = 'http://m.sh.lianjia.com/'
    # for each in ('http://www.kuaidaili.com/free/inha/{}/', 'http://www.kuaidaili.com/free/intr/{}/'):
    #     p = ProxiesThread(each, test_url)
    #     p.start()
    # for city in City_surls:
    #     th = threading.Thread(target=scrapy_districts_from_city, args=(city))
    #     th.start()
    for tar_city in City_kw_dict:
        while True:
            try:
                categories = scrapy_categories_from_city(tar_city)
                break
            except TypeError:
                continue

        for cate in categories:
            scrapy_pages(categories[cate], tar_city, cate)

        # count = 0
        # while True:
        #     if sql_tasks:
        #         count = 0
        #         job = sql_tasks.pop()
        #         try:
        #             sql.cursor().execute('INSERT INTO Jobs VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)', (*job,))
        #             print('{} inserted.'.format(job))
        #             sql.commit()
        #         except sqlite3.IntegrityError as err:
        #             continue
                    # loop = asyncio.get_event_loop()
                    # loop.run_until_complete(apply_city())

                    # json_str = json.dumps(City_urls)
                    # with open('City_urls.json', 'w') as f:
                    #     f.write(json_str)
            # else:
            #     time.sleep(6)
            #     count += 1
            #     if count > 10:
            #         break
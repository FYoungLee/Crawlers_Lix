"""
    从天天基金网下载所有基金数据，并存入数据库sqlite。
    作者： Fyoung Lix
    日期： 2016年10月27日
"""

from urllib.request import urlopen, Request
from urllib.parse import urlencode
from bs4 import BeautifulSoup as bsoup
import requests
import sqlite3
import time

conn = sqlite3.connect('Funds.sqlite')
cursor = conn.cursor()
mainsource = 'http://fund.eastmoney.com'

FundINFO = '基金数据'
FuValDB = '历史净值'
FuIDDB = '基金库'
ConDB = '连接库'
MaIDDB = '基金经理'
CoIDDB = '基金公司'


# 创建数据库
def create_tables(fundinfo=FundINFO, fundval=FuValDB, fundid=FuIDDB, condb = ConDB, managerid=MaIDDB, companyid=CoIDDB):
    cursor.execute('CREATE TABLE {}'
                   '(基金代码 TEXT NOT NULL PRIMARY KEY, 基金类型 TEXT, 成立规模 FLOAT, 当前规模 FLOAT)'
                   .format(fundinfo))
    cursor.execute('CREATE TABLE {}'
                   '(记录代码 INGEGER NOT NULL PRIMARY KEY, 日期 DATE, 单位净值 FLOAT, 累计净值 FLOAT, 基金代码 TEXT)'
                   .format(fundval))
    cursor.execute('CREATE TABLE {}(基金代码 TEXT NOT NULL PRIMARY KEY,名称 TEXT)'.format(fundid))
    cursor.execute('CREATE TABLE {}(基金代码 TEXT NOT NULL, 经理代码 TEXT, 公司代码 TEXT)'.format(condb))
    cursor.execute('CREATE TABLE {}(经理代码 TEXT NOT NULL PRIMARY KEY, 姓名 TEXT)'.format(managerid))
    cursor.execute('CREATE TABLE {}(公司代码 TEXT NOT NULL PRIMARY KEY, 名称 TEXT)'.format(companyid))


# 获取基金ID和名称的代码:
def dl_fundid_in2(tbname=FuIDDB, mod=1):
    if mod == 1:
        rst = requests.get('http://fund.eastmoney.com/js/fundcode_search.js').text
        rst = rst[rst.find('[') + 1:rst.rfind(']') - 1].replace('\"', '').replace('[', '').split('],')
        for ea in rst:
            ea = ea.split(',')
            cmd = 'INSERT INTO {} VALUES(\'{}\',\'{}\')'.format(tbname, ea[0], ea[2])
            # print(cmd)
            cursor.execute(cmd)
    elif mod == 0:
        subs = mainsource + '/Data/Fund_JJJZ_Data.aspx'
        rst = requests.get(subs + '?page=1,9999').text
        rst = rst[rst.find('datas:[') + 7:rst.rfind('count:') - 2].replace('[', '').replace('\"', '').split('],')
        for ea in rst:
            cursor.execute('INSERT INTO {} VALUES(\'{}\',\'{}\')'.format(tbname, ea[0], ea[1]))
    print('基金代码数据更新完毕')
    conn.commit()


# 基金净值数据下载
def dl_fundval(fundid, tbname=FuValDB):
    subs = mainsource + '/f10/F10DataApi.aspx'
    postdat = {'type': 'lsjz', 'page': 1, 'per': 9999, 'code': fundid}

    def getrespon(po):
        try:
            return bsoup(urlopen(subs, data=urlencode(po).encode('utf-8')), 'lxml')
        except BaseException as err:
            print(err)
            return None

    bsobj = getrespon(postdat)
    while bsobj is None:
        time.sleep(5)
        bsobj = getrespon(postdat)
    print('下载\'' + fundid + '\'中...')
    for each in bsobj.find('tbody').findAll('tr'):
        each = each.findAll('td')[:4]
        try:
            day, val1, val2 = each[0].text[:10].replace('-',''), each[1].text, each[2].text
        except IndexError as err:
            print(err, each)
            continue
        if val2.replace('.', '').isdigit is False:
            if val1.replace('.', '').isdigit is False:
                continue
            val2 = val1
        cmd = 'INSERT INTO {} VALUES({},{},{},{},\'{}\')'.format(tbname, day + fundid, day, val1, val2, fundid)
        try:
            cursor.execute(cmd)
        except BaseException as err:
            print(err, cmd)
    conn.commit()
    print(fundid,'基金净值更新完毕')


def update_funddb(startid='000001'):
    # 从基金数据库中导出到内存
    fdb = cursor.execute('SELECT * FROM {} WHERE 基金代码>=\'{}\''.format(FuIDDB, startid)).fetchall()

    def get_urlinfo(url):
        try:
            return bsoup(urlopen(url), 'lxml')
        except BaseException as err:
            print(err)
            return None

    # 分步提取基金数据库中基金代码
    for ea in fdb:
        # 获取基金信息页面
        subs = mainsource + '/f10/jbgk_{}.html'.format(ea[0])
        print('进入网页', subs)
        bsobj = get_urlinfo(subs)
        while bsobj is None:
            time.sleep(3)
            bsobj = get_urlinfo(subs)
        try:
            bsobj = bsobj.find('table', {'class': 'info w790'}).findAll('tr')
        except BaseException as er:
            print(er, '信息页面提取错误')
            continue

        # 提取基金经理信息
        for e in bsobj[5].findAll('a')[:-1]:
            manid = e['href']
            manid = manid[manid.rfind('/')+1:manid.rfind('.')]
            # 插入经理数据: 经理代码、经理姓名
            cmd = 'INSERT INTO {} VALUES(\'{}\',\'{}\')'.format(MaIDDB, manid, e.text)
            # print(cmd)
            try:
                cursor.execute(cmd)
            except BaseException as er:
                print(er, cmd)
            # 插入连接库数据: 基金代码、经理代码
            cmd = 'INSERT INTO {} (基金代码, 经理代码) VALUES(\'{}\',\'{}\')'.format(ConDB, ea[0], manid)
            # print(cmd)
            try:
                cursor.execute(cmd)
            except BaseException as err:
                print(err, cmd)
        # 插入基金基本信息: 基金代码、基金类型、成立规模、当前规模
        bgm = bsobj[2].findAll('td')[1].text[14:-2]
        ngm = bsobj[3].findAll('td')[0].text[:bsobj[3].findAll('td')[0].text.find('亿')].replace(',','')
        if bgm == '':
            bgm = ngm
        cmd = 'INSERT INTO {} VALUES (\'{}\',\'{}\',{},{})'\
            .format(FundINFO, ea[0], bsobj[1].findAll('td')[1].text, bgm, ngm)
        print(cmd)
        try:
            cursor.execute(cmd)
        except BaseException as er:
            print(er, cmd)

        comid = bsobj[4].find('a')['href']
        comid = comid[comid.rfind('/')+1:comid.rfind('.')]
        # 插入公司数据: 公司代码、公司名称
        cmd = 'INSERT INTO {} VALUES(\'{}\',\'{}\')'.format(CoIDDB, comid, bsobj[4].find('a').text)
        # print(cmd)
        try:
            cursor.execute(cmd)
        except BaseException as er:
            print(er, cmd)
        # 更新连接数据, 加入基金公司代码
        cmd = 'UPDATE {} SET 公司代码=\'{}\' WHERE 基金代码=\'{}\''.format(ConDB, comid, ea[0])
        # print(cmd)
        cursor.execute(cmd)
        conn.commit()
        dl_fundval(ea[0])
    print('所有数据更新完毕')

# create_tables()
# dl_fundid_in2()
update_funddb(startid='002374')
conn.close()



############
# 代码回收站:#
############

##############################################################################################################
# 获取基金经理ID的代码:
# def dl_managerid_in2(tbname):
#     subs = mainsource + '/Data/FundDataPortfolio_Interface.aspx'
#     rst = requests.post(subs, {'dt': '14', 'pn': '3000'}).text
#     rst = rst[rst.find('[') + 1:rst.rfind(']')].replace('[', '').split('],')
#     for ea in rst:
#         mdat = ea.replace('\"', '').split(',')[:2]
#         cursor.execute('INSERT INTO {} VALUES({},{})'.format(tbname, mdat[0], mdat[1]))
#     print('基金经理代码数据更新完毕')
#     conn.commit()
##############################################################################################################

##############################################################################################################
# 获取所有基金历史数据的代码:
# cursor.execute('SELECT * FROM FundsDB')
# db = cursor.fetchall()
#
# url = mainsource + '/f10/F10DataApi.aspx'
# postdat = {}
# postdat['type'] = 'lsjz'
# postdat['page'] = 1
# postdat['per'] = 9999
#
# def getrespon(id):
#     postdat['code'] = id
#     data = urlencode(postdat)
#     request = Request(url)
#     try:
#         response = urlopen(request, data=data.encode('utf-8'))
#     except BaseException as err:
#         print(err)
#         return None
#     return bsoup(response, 'lxml')
#
# for eachfund in db:
#     bsobj = getrespon(eachfund[1])
#     while bsobj is None:
#         time.sleep(5)
#         bsobj = getrespon(eachfund[1])
#     print('Downloading:' + eachfund[2])
#     for each in bsobj.find('tbody').findAll('tr'):
#         insertdat = each.findAll('td')[:4]
#         try:
#             day = insertdat[0].text.replace('-', '')
#             val1 = insertdat[1].text
#             val2 = insertdat[2].text
#             rat = insertdat[3].text
#         except IndexError as err:
#             print(err)
#             continue
#         cmd = 'INSERT INTO ValuesDB VALUES({},{},{},\'{}\',{})'.format(day, val1, val2, rat, eachfund[0])
#         try:
#             cursor.execute(cmd)
#         except sqlite3.OperationalError as err:
#             print(err)
#     conn.commit()
#
# conn.close()
# print('Mission complete!')
##############################################################################################################

##############################################################################################################
# 数据迁移
##############################################################################################################
# fundsdb = cursor.execute('SELECT ID FROM FundsDB').fetchall()
# # cursor.execute('CREATE TABLE ValDB(vid NOT NULL PRIMARY KEY, 日期 DATE, 单位净值 FLOAT, 累计净值 FLOAT, '
# #                 '日增长率 TEXT, ID INTEGER NOT NULL, FOREIGN KEY(ID) REFERENCES FundsDB(ID));')
# for each in fundsdb:
#     cmd = 'SELECT * FROM ValuesDB WHERE ID={}'.format(each[0])
#     tempdat = cursor.execute(cmd).fetchall()
#     for eachtempdat in tempdat:
#         cmd = 'INSERT INTO ValDB VALUES({},{},{},{},\'{}\',{})'.\
#             format(int(str(eachtempdat[0])+str(eachtempdat[4])),
#                    eachtempdat[0],eachtempdat[1],eachtempdat[2],eachtempdat[3],eachtempdat[4])
#         try:
#             cursor.execute(cmd)
#         except sqlite3.IntegrityError as err:
#             print(err)
#             continue
#     conn.commit()
#     print(each,'Done')
##############################################################################################################
"""
    从天天基金网，根据基金的排名情况，选出业绩长期优良的基金，然后下载其数据，最后制作成html文件以供查阅。
    作者： Fyoung Lix
    日期： 2016年11月2日
    版本： v1.0
"""
# -*- coding: utf-8 -*-
import requests
import re
import json
import os
from urllib.request import urlopen
from datetime import date, datetime


# 导出到一个html文件
def parse_html(data=None):
    with open('MyFunds.html', 'w') as file:
        print('<html lang="zh-CN">', file=file)
        print('<meta charset="UTF-8">', file=file)
        print('<table border="10">', file=file)
        print('<tr>', file=file)
        print('<td>基金名称</td>', file=file)
        print('<td>评分</td>', file=file)
        print('<td>单位净值</td>', file=file)
        print('<td>累计净值</td>', file=file)
        print('<td>日涨幅</td>', file=file)
        print('<td>周涨幅</td>', file=file)
        print('<td>月涨幅</td>', file=file)
        print('<td>季涨幅</td>', file=file)
        print('<td>半年涨幅</td>', file=file)
        print('<td>一年涨幅</td>', file=file)
        # print('<td>两年涨幅</td>', file=file)
        # print('<td>三年涨幅</td>', file=file)
        print('<td>基金规模</td>', file=file)
        print('<td>基金经理</td>', file=file)
        print('</tr>', file=file)
        for each in data:
            print('<tr>', file=file)
            print('<td><a href="{}" target="_blank">{}</a></td>'
                  .format('http://fund.eastmoney.com/{}.html'.format(each['ID']),each['name']), file=file)
            print('<td>{}</td>'.format(each['score']), file=file)
            print('<td>{}</td>'.format(each.get_cval(datetime.now().timestamp())), file=file)
            print('<td>{}</td>'.format(each.get_tval(datetime.now().timestamp())), file=file)
            print('<td>{}</td>'.format(each.get_UD(0)), file=file)
            print('<td>{}</td>'.format(each.get_UD(7)), file=file)
            print('<td>{}</td>'.format(each.get_UD(31)), file=file)
            print('<td>{}</td>'.format(each.get_UD(92)), file=file)
            print('<td>{}</td>'.format(each.get_UD(183)), file=file)
            print('<td>{}</td>'.format(each.get_UD(365)), file=file)
            # print('<td>{}</td>'.format(each.get_UD(730)), file=file)
            # print('<td>{}</td>'.format(each.get_UD(1095)), file=file)
            print('<td>{}</td>'.format(each['scale'][-1][-1]), file=file)
            print('<td>', file=file)
            for em in each['manager']:
                print('<a href="{}" target="_blank">{}({})</a><br>'
                      .format('http://fund.eastmoney.com/manager/{}.html'.format(em[0]), em[1], em[2]), file=file)
            print('</td>', file=file)
            print('</tr>', file=file)
        print('</table>', file=file)
        print('</html>', file=file)
        print(os.getcwd()+'/'+file.name, '保存成功')


class FundData(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_cval(self, day):
        """
            获得单位净值
        """
        try:
            return self['value'][date.fromtimestamp(day).isoformat()]
        except KeyError:
            return self.get_tval(day - 86400)

    def get_tval(self, day):
        """
            获得累计净值
        """
        try:
            return self['tvalue'][date.fromtimestamp(day).isoformat()]
        except KeyError:
            return self.get_tval(day - 86400)

    def get_UD(self, days):
        """
            获得增长幅度
        """
        checkdays = [ed for ed in self['value']]
        checkdays.sort(reverse=True)
        today = datetime.now().timestamp()
        tarday = today - days * 86400
        targetday = date.fromtimestamp(tarday).isoformat()
        t = 1
        for each in checkdays:
            if targetday >= each:
                break
            t += 1
        checkdays = checkdays[:t]
        t = 0
        for each in checkdays:
            t += self['value'][each][1]
        return round(t, 2)

    def get_managers(self):
        """
            获取经理信息
        """
        ret = ''
        for each in self['manager']:
            ret += '{}({})|'.format(each[1], each[2])
        return ret[:-1]

    def display(self):
        """
            控制台输出
        """
        print('[{}]{}({})'.format(self['ID'], self['name'], self['score']), end='\t\t\t')
        print('净值', self.get_cval(datetime.now().timestamp()), end='\t')
        print('累计', self.get_tval(datetime.now().timestamp()), end='\t')
        print('一周', self.get_UD(7), end='\t')
        print('半月', self.get_UD(15), end='\t')
        print('一月', self.get_UD(31), end='\t')
        print('季度', self.get_UD(92), end='\t')
        print('半年', self.get_UD(183), end='\t')
        print('规模', self['scale'][-1][-1], end='\t')
        print(self.get_managers())


class FundSelctor:
    def __init__(self):
        self.s_engine = 'http://fund.eastmoney.com/data/rankhandler.aspx'

    def dl_data(self, sc):
        """
            传入一个排序参数（zzf＝周涨幅， 1yzf＝1月zf，1nzf＝1年涨幅，以此类推)
            返回一个字符串内容结果
        """
        if not re.match(r'^zzf$|^\d{1,2}yzf$|^\dnzf$', sc):
            print('错误参数')
            return
        postr = {'op': 'ph', 'sc': sc, 'pn': 10000, 'st': 'desc'}
        return requests.post(self.s_engine, data=postr).text

    def slice_data(self, string):
        """
            接收dl_data获得的字符串结果，返回一个分切好的Dict。
            datas   为基金代码
            records 为全部基金总数
        """
        datas = [re.match(r'\d{6}', each.replace('\"', '')).group() for each in
                 string[string.find('datas:') + 7:string.rfind(',\"]')].split('\",')]
        records = string[string.rfind('allRecords'):].split(',')[0].split(':')[1]
        return {'datas': datas, 'records': int(records)}

    def trunc_data(self, data, rankpercent):
        """
            接收一个字典容器，接收一个需要的排名百分比
            返回排名好后的结果的List
        """
        data['records'] = int(data['records'] / 100 * rankpercent)
        data['datas'] = data['datas'][:data['records']]
        return data['datas']

    def compare_data(self, datal, datar):
        """
            比较两个List，返回其交集。
        """
        return [ea for ea in datal if ea in datar]

    def get_list(self, rankpercent, *args):
        """
            接收一个排名百分比数值，一个List的搜索关键字
            返回一个提炼基金名单
        """
        ret = []
        compr = None
        for each in args:
            ret.append(self.trunc_data(self.slice_data(self.dl_data(each)), rankpercent))
            print('关键字', each, '搜索完毕')
        for each in range(len(ret)):
            if each == 0:
                compr = ret[0]
                continue
            compr = self.compare_data(compr, ret[each])
        return compr

    def dl_fund_info(self, fid):
        pages = urlopen("http://fund.eastmoney.com/pingzhongdata/{}.js".format(fid)).read().decode('utf-8').split(';')
        # 基金名字
        fname = pages[1][pages[1].find('\"'):].replace('\"', '')
        # 净值库
        cvaldb = {}
        tval = [each.split(',') for each in pages[13][pages[13].find('[') + 1:-1].replace('{', '').split('},')]
        for eachday in tval:
            try:
                cvaldb[date.fromtimestamp(int(eachday[0].split(':')[1]) / 1000).isoformat()] \
                    = [float(eachday[1].split(':')[1]), float(eachday[2].split(':')[1])]
            except IndexError as err:
                print('读取基金净值出错：', err)

        # continue
        # # 累计净值库
        tvaldb = {}
        tval = [each.split(',') for each in pages[14][pages[14].find('[') + 1:-2].replace('[', '').split('],')]
        for eachday in tval:
            try:
                tvaldb[date.fromtimestamp(int(eachday[0]) / 1000).isoformat()] = float(eachday[1])
            except IndexError as err:
                print('读取累计净值出错：', err)
        # 业绩评分
        try:
            fund_score = re.match(r'.*avr":"(\d*.\d*)"', pages[21].replace('\n', '')).group(1)
        except:
            fund_score = 0
        # 基金经理, 经理评分
        managers = []
        managerpage = pages[22].replace('\n', '')
        manager_id = re.findall(r'"id":"(\d*)"', managerpage)
        manager_name = re.findall(r'"name":"(\w*)"', managerpage)
        manager_score = re.findall(r'"avr":"(\d*.\d*|\w+)"', managerpage)
        for each in range(len(manager_id)):
            if manager_score[each] == '暂无数据':
                manager_score[each] = 0
            managers.append([manager_id[each], manager_name[each], manager_score[each]])
        # 基金规模
        scalepage = pages[18].replace('\n', '')
        scaledate = re.match(r'.*categories":\[(.+?)\]', scalepage).group(1).split(',')
        eachscale = [each.split(',')[0].split(':')[1] for each in
                     re.match(r'.*series":\[(.+?)\]', scalepage).group(1).replace('{', '').split('},')]
        scales = [[scaledate[e], eachscale[e]] for e in range(len(scaledate))]

        return {'ID': fid, 'name': fname, 'value': cvaldb, 'tvalue': tvaldb,
                'score': fund_score, 'manager': managers, 'scale': scales}

if __name__ == '__main__':
    while True:
        print('1。更新优选基金数据')
        print('2。制作优选基金文档')
        print('3。退出')
        cmd = input('输入指令：')
        if cmd == '1':
            funds = []
            fs = FundSelctor()
            try:
                pm = int(input('排名筛选百分比 (1 - 50)>> '))
            except ValueError:
                print('请输入正确的数值')
                continue
            if pm < 1 or pm > 50:
                print('超出范围')
                continue
            mylist = fs.get_list(pm, '1nzf', '6yzf', '3yzf', '1yzf', 'zzf')
            for each in mylist:
                print(each, '下载中...')
                sg = fs.dl_fund_info(each)
                funds.append(sg)
                print(each, '下载完毕')
            with open('myfunds.json','w') as f:
                f.write(json.dumps(funds))
        elif cmd == '2':
            myfunds = []
            with open('myfunds.json', 'r') as f:
                funds = json.loads(f.read())
                for each in funds:
                    fund = FundData()
                    fund.update(each)
                    myfunds.append(fund)
            myfunds.sort(key=lambda x:x['name'], reverse=True)
            parse_html(myfunds)
        elif cmd == '3':
            exit()
        else:
            print('错误指令')



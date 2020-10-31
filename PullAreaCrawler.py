import copy
import json
import multiprocessing
import os
import random
import re
import traceback
from time import sleep

import requests
import pymysql
from bs4 import BeautifulSoup

agent_list = [
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; U; Android 4.0.2; en-us; Galaxy Nexus Build/ICL53F) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 SE 2.X MetaSr 1.0",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.105 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36",
]

def patch_href(base_url: str, href: str):
    return re.sub('/[a-z0-9]+\.html$', "/%s" % href, base_url)

def connect():
    return pymysql.connect(host='127.0.0.1', port=3306, user='weskiller', passwd='123456', db='area', charset='utf8',autocommit=True)

def html_root(url):
    return url.replace("http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019","/data/tjsj")

def save_html(url,content):
    path = html_root(url)
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        os.mkdir(directory, 0o755)
    try:
        with open(path,'w+',encoding='gbk') as f:
            f.write(content.replace('\ufffd',''))
    except BaseException as e:
        print(" write html to file failed (%s) (%s)" % (repr(e),url))

def get_heidong_proxy(proxies:list,num):
    url = 'http://ip.ipjldl.com/index.php/api/entry?method=proxyServer.hdtiqu_api_url&packid=7&fa=3&groupid=0&fetch_key=&time=6&qty=%s&port=1&format=json&ss=5&css=&dt=&pro=&city=&usertype=4' % num
    result = requests.get(url,headers={'User-Agent': get_agent()})
    try:
        data = json.loads(result.text)
        for i in data["data"]:
            if len(proxies) > 0:
                proxies.pop(0)
            proxies.append("http://%s:%s" % (i["IP"],i["Port"]))
        return True
    except BaseException as e:
        print("INVOKE PROXY FAILED %s" % repr(e))
        return False

def flush_http_proxy():
    while not get_heidong_proxy(Area.proxies,16):
        sleep(3)

def get_http_proxy():
    if len(Area.proxies) > 0:
        return Area.proxies[0]
    else:
        flush_http_proxy()
        return get_http_proxy()

def get_agent():
    return random.choice(agent_list)

def fails():
    if Area.fails > 3:
        Area.fails = 0
        Area.proxies.pop(0)
    else:
        sleep(Area.fails * 1)
    Area.fails += 1

def success():
    if Area.fails > 1:
       Area.fails -= 1

def create_table():
    cur = connect().cursor()
    cur.execute("DROP TABLE IF EXISTS `%s`" % Area.table_name)
    cur.execute("""
    CREATE TABLE `%s` (
      `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
      `level` int(10) DEFAULT NULL,
      `code` varchar(12) DEFAULT NULL,
      `short_code` varchar (12) DEFAULT NULL,
      `parent` varchar(12) DEFAULT NULL,
      `path` json DEFAULT NULL,
      `name` varchar(32) DEFAULT NULL,
      `merger_name` varchar(255) DEFAULT NULL,
      UNIQUE KEY (`code`),
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """ % Area.table_name)

class Area(object):
    table_name = "cn_area"
    fails = 0
    proxies = []
    verify_string = "统计用区划代码"
    connect = connect()
    tag = ["tr.provincetr", "tr.citytr", "tr.countytr", "tr.towntr", "tr.villagetr"]
    def __init__(self, short_code: str, path: [str], level: int, merger_name: str, name: str, href: str):
        self.short_code: str = short_code
        self.href: str = href
        self.code: str = short_code + "000000000000"[len(short_code):]
        if len(path) > 0:
            self.parent: str = path[-1]
        else:
            self.parent: str = "0"
        self.path: [] = path
        path.append(self.code)
        self.level: int = level
        self.name: str = name
        if merger_name != "":
            self.merger_name: str = ("%s,%s" % (merger_name, name))
        else:
            self.merger_name: str = name

    def save(self):
        cur = Area.connect.cursor()
        sql = (
            'INSERT INTO `%s` (`short_code`,`code`, `level`,`parent`, `name`, `merger_name`,`path`) VALUES("%s","%s", %s, "%s", "%s","%s",\'%s\')' % (
            Area.table_name,self.short_code, self.code, self.level, self.parent, self.name, self.merger_name,
            json.dumps(self.path)))
        try:
            cur.execute(sql)
        except BaseException as e:
            print(repr(e))

    def pull(self):
        self.view()
        self.save()
        if self.href != "":
            content = html_get(self.href)

            while (not isinstance(content,str)) or content.find(Area.verify_string) < 0:
                fails()
                content = html_get(self.href)

            success()
            save_html(self.href,content)

            soup = BeautifulSoup(content, 'lxml')
            level = 0
            for key, value in enumerate(Area.tag):
                data = soup.select(Area.tag[key])
                if len(data) > 0:
                    level = key + 1
                for area in data:
                    td = area.find_all("td")
                    full_code: str = td[0].get_text()
                    if level > 3:
                        code: str = full_code[:3 * 2 + (level - 3) * 3]
                    else:
                        code: str = full_code[:level * 2]
                    if td[0].a is not None and td[0].a["href"] is not None:
                        href = patch_href(self.href, td[0].a["href"])
                    else:
                        href = ""
                    if len(td) == 2:  # 城/镇
                        name: str = td[1].get_text()
                    elif len(td) == 3:  # 城/乡
                        name: str = td[2].get_text()
                    else:
                        raise BaseException("unexpect html content")
                    this = Area(short_code=code, path=copy.deepcopy(self.path), level=level,
                                merger_name=self.merger_name, name=name, href=href)
                    this.pull()

    def view(self):
        print(self.__dict__)

def html_get(url):
    try:
        print("%s => %s" % ("pull", url))
        body = requests.get(url,
            timeout= 3,
            headers={'User-Agent': get_agent()},
            proxies={'http': get_http_proxy()}
        )
        body.encoding = 'GBK'
        print(body.text)
    except BaseException as e:
        print(repr(e))
        fails()
    else:
        return body.text
#多进程执行入口
def fetch(province :map):
    Area.connect = connect()
    area = Area(
        short_code=province["short_code"],
        path=province["path"],
        level=province["level"],
        merger_name=province["merger_name"],
        name=province["name"],
        href=province["href"]
    )
    try:
        area.pull()
    except BaseException as e:
        print(repr(e))
        traceback.print_exc(file=open("error.log",'a+'))


def distribute(url: str):
    content = html_get(url)
    while (not isinstance(content, str)) or content.find(Area.verify_string) < 0:
        fails()
        content = html_get(url)
    success()
    print("%s => %s success" % ("pull", url))
    save_html(url,content)
    soup = BeautifulSoup(content, 'lxml')
    data = soup.select(Area.tag[0])
    pool = multiprocessing.Pool(64)
    for record in data:
        for row in record.find_all('td'):
            if row.get_text():
                if row.a is not None:
                    if row.a is not None and row.a["href"] is not None:
                        href = patch_href(url, row.a["href"])
                    else:
                        href = ""
                    area = {
                        "short_code": re.split('[./]', href)[-2],
                        "path": copy.deepcopy([]),
                        "level": 1,
                        "merger_name": "",
                        "name": row.get_text(),
                        "href": href,
                    }
                    pool.apply_async(fetch, (area,))
    pool.close()
    pool.join()

if __name__ == '__main__':
    create_table()
    get_heidong_proxy(Area.proxies,1)
    baseUrl = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/index.html"
    distribute(baseUrl)
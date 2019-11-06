# 库函数导入
import requests
from lxml import etree
import csv
import time
import pandas as pd
from queue import Queue
from threading import Thread


# 网页爬取函数
# 下面加入了num_retries这个参数，经过测试网络正常一般最多retry一次就能获得结果
def getUrl(url,num_retries = 5):
    headers = {'User-Agent':"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"}
    try:
        response = requests.get(url,headers = headers)
        response.encoding = 'GBK'
        data = response.text
        return data
    except Exception as e:
        if num_retries > 0:
            time.sleep(10)
            print(url)
            print("requests fail, retry!")
            return getUrl(url,num_retries-1) #递归调用
        else:
            print("retry fail!")
            print("error: %s" % e + " " + url)
            return #返回空值，程序运行报错


# 获取街道代码函数---多线程实现
def getTown(url_list):
    queue_town = Queue() #队列
    thread_num = 50 #线程数
    town = [] #记录街道信息的字典（全局）
    
    def produce_url(url_list):
        for url in url_list:
            queue_town.put(url) # 生成URL存入队列，等待其他线程提取
    
    def getData():
        while not queue_town.empty(): # 保证url遍历结束后能退出线程
            url = queue_town.get() # 从队列中获取URL
            data = getUrl(url)
            selector = etree.HTML(data)
            townList = selector.xpath('//tr[@class="towntr"]')
            #下面是爬取每个区的代码、URL
            for i in townList:
                townCode = i.xpath('td[1]/a/text()')
                townLink = i.xpath('td[1]/a/@href')
                townName = i.xpath('td[2]/a/text()')
                #上面得到的是列表形式的，下面将其每一个用字典存储
                for j in range(len(townLink)):
                    # 中山市、东莞市的处理
                    if url == 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/44/4419.html' or url == 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/44/4420.html':
                        townURL = url[:-9] + townLink[j]
                    else:
                        townURL = url[:-11] + townLink[j]
                    town.append({'code':townCode[j],'name':townName[j],'link':townURL,'type':"town"})
                
    def run(url_list):
        produce_url(url_list)
    
        ths = []
        for _ in range(thread_num):
            th = Thread(target = getData)
            th.start()
            ths.append(th)
        for th in ths:
            th.join()
            
    run(url_list)
    return town


###########################
#街道信息获取
#中山市、东莞市的特殊处理（他们的链接在df_city中）
url_list = list()
df = pd.read_csv("result.csv",header=0)
df_county = pd.read_csv("result.csv",header=0)
df_city = pd.read_csv("city.csv",header=0)
for url in df_county['link']:
    url_list.append(url)
town_link_list = df_city[df_city['name'].isin(['中山市','东莞市'])]['link'].values
for town_link in town_link_list:
    url_list.append(town_link)

with open('town_url_list.txt', 'w') as f:
    for i in url_list:
        f.write(i + '\n')
# town = getTown(url_list)
# df_town = pd.DataFrame(town)
# # 排序:由于多线程的关系，数据的顺序已经被打乱，所以这里按照街道代码进行“升序”排序。
# df_town_sorted = df_town.sort_values(by = ['code']) #按1列进行升序排序
# df_town_sorted.info()
# # 信息写入csv文件
# df_town_sorted.to_csv('town.csv', sep=',', header=True, index=False)

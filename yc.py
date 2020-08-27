# -*- coding: utf-8 -*-
'''
@datetime: 2020/8/27 13:54
@author: Jack Luo
@job:
//todo:
'''
import requests
from scrapy import Selector
from fake_useragent import UserAgent
import re
import time
from sqlalchemy import create_engine
import pandas as pd

ua=UserAgent()
headers={'User-Agent':ua.random}
engine = create_engine("mysql+pymysql://root:123456@localhost:3306/fang", encoding='utf8')


def get_response_sel_from_each_page(page):
    url=f'https://chongqing.anjuke.com/sale/yongchuanqu/p{page}/'
    response=requests.get(url,headers=headers)
    if '访问验证' in response.text:
        print(f'page{page}被反爬ban')
    time.sleep(0.5)
    fangs=Selector(response=response).css('li.list-item')
    return fangs


def get_house_info_from_fangs(fangs):
    
    houses_dataframe=pd.DataFrame(columns=('community_name','location',
                                 'house_nums','area','built_year','total_price','unit_price'))
    for fang in fangs:
        fang_infos={}
        try:
            infos=fang.css('li.list-item span::text').extract()
            comm_name=infos[4]
            comm_name=re.match('\W*?(\w+(\(\w+\))?)\W*?',comm_name).group(1)
            fang_infos['community_name']=comm_name

            location=infos[4]    
            location=re.match('.*?-(\w+)-.*?',location.strip(),flags=re.DOTALL).group(1)
            fang_infos['location']=location

            house_nums=infos[0]
            fang_infos['house_nums']=house_nums
            area=infos[1]
            area=int(re.match('(\d+)',area).group(1))
            fang_infos['area']=area

            built_year=infos[3]
            built_year=int(re.match('(\d+)',built_year).group(1))
            fang_infos['built_year']=built_year

            total_price=int(float(fang.css('span.price-det strong::text').extract_first()))
            fang_infos['total_price']=total_price

            unit_price=fang.css('span.unit-price::text').extract_first()
            unit_price=int(re.match('\d+',unit_price).group(0))
            fang_infos['unit_price']=unit_price
        except:
            print(f'   获取失败！')

        houses_dataframe=houses_dataframe.append(fang_infos,ignore_index=True)
    return houses_dataframe

def dataframe_to_sql(dataframe,table_name,index_true_or_false=False):
    dataframe.to_sql(f'{table_name}', con=engine,if_exists='append',index=index_true_or_false)

def main():
    for page in range(1,51):
        fangs=get_response_sel_from_each_page(page)
        dataframe=get_house_info_from_fangs(fangs)
        try:
            dataframe_to_sql(dataframe,'yongchuan')
            print(f'page{page}数据已写入table：yongchuan')
        except:
            print(f'page{page}数据写入失败')

if __name__ == '__main__':
    main()

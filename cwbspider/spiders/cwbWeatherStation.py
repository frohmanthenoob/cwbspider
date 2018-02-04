# -*- coding: utf-8 -*-
import scrapy
import json
import collections

def reduceParameter(item):
    temp = collections.defaultdict(str)
    for key in item.keys():
        if key == 'parameter':
            for parameter in item[key]:
                temp[parameter['parameterName']] = parameter['parameterValue']
        elif key == 'weatherElement':
            for weatherElement in item[key]:
                temp[weatherElement['elementName']] = weatherElement['elementValue']
        elif key == 'time':
            temp['obsTime'] = item[key]['obsTime']
        else:
            temp[key]=item[key]
    return dict(temp)

class CwbweatherstationSpider(scrapy.Spider):
    name = 'cwbWeatherStation'
    allowed_domains = ['https://opendata.cwb.gov.tw/']
    start_urls = ['https://opendata.cwb.gov.tw/api/v1/rest/datastore/O-A0003-001?Authorization=CWB-30468991-9E32-4087-B504-1FEFFB0DF896']

    def parse(self, response):
        mytext = response.xpath('//p/text()').extract_first()
        myjson = json.loads(mytext)
        mydata = myjson['records']['location']
        flatdata = list(map(reduceParameter,mydata))
        for data in flatdata:
            _dict = { key:value for key, value in data.items() }
            _dict.update({'mongodb_collection':'cwbWeatherStation'})
            yield _dict

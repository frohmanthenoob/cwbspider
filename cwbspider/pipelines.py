# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient

MONGODB_SERVER = 'localhost'
MONGODB_PORT = 27017
MONGODB_DB = 'cwb'
MONGODB_COLLECTION_automaticRainfallStation = 'automaticRainfallStation'
MONGODB_COLLECTION_automaticWeatherStation = 'automaticWeatherStation'
MONGODB_COLLECTION_cwbWeatherStation = 'cwbWeatherStation'
MONGODB_COLLECTION_cwbRainfallStation = 'cwbRainfallStation'
MONGODB_COLLECTION = 'mongodb_collection'

class CwbspiderPipeline(object):
    def process_item(self, item, spider):
        return item

    def __init__(self):
        connection = MongoClient( MONGODB_SERVER, MONGODB_PORT )
        self.db = connection[ MONGODB_DB ]
        self.collection = self.db[MONGODB_COLLECTION]

    def process_item(self, item, spider):
        _dict = dict(item)
        
        if _dict[MONGODB_COLLECTION] == 'automaticRainfallStation':
            self.collection = self.db[MONGODB_COLLECTION_automaticRainfallStation]
        
        if _dict[MONGODB_COLLECTION] == 'cwbRainfallStation':
            self.collection = self.db[MONGODB_COLLECTION_cwbRainfallStation]
            
        if _dict[MONGODB_COLLECTION] == 'automaticWeatherStation':
            self.collection = self.db[MONGODB_COLLECTION_automaticWeatherStation]

        if _dict[MONGODB_COLLECTION] == 'cwbWeatherStation':
            self.collection = self.db[MONGODB_COLLECTION_cwbWeatherStation]
            

            
        del _dict[MONGODB_COLLECTION]
        self.collection.insert(_dict)
        
        return item
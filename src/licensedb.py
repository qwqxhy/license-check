# -*- coding: utf-8 -*-
from pymongo import MongoClient

from localconfig import *

import threading

class MongoClintSingleton:
    _instance = None
    lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance:
            return cls._instance
        else:
            with cls.lock:
                cls._instance = MongoClient(local_mongodb_url)
            return cls._instance

    def __init__(self):
        pass
   

class LicenseDB(object):

    def __init__(self):
        self.client = MongoClintSingleton()
        self.db = self.client[local_mongodb_db]
        self.coll_license_info = self.db['license_info']
        self.coll_license_term = self.db['license_term']

    def get_license_term_by_key(self, key):
        lic = self.coll_license_term.find_one({'license_key': key}, {"_id": 0})
        return lic if lic else None

    def get_license_info_by_key(self, license_key):
        license_info = self.coll_license_info.find_one({'key': license_key}, {'_id': 0, 'key': 0})
        return license_info if license_info else {}
    
    def get_license_category_by_key(self, license_key):
        license_category = self.coll_license_info.find_one({'key': license_key}, {'_id': 0, 'category': 1})
        if license_category:
            return license_category['category'] if license_category['category'] else ''
        else:
            return ''

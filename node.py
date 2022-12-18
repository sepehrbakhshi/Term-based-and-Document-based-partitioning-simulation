# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 18:12:36 2022

@author: 3pehr
"""

class node:
    
    def __init__(self):
        self.records = []
        
    
    def set_record(self, records):
        self.records = records
 
    def get_record(self):
        return self.records
    
    def get_record_terms(self):
        return
        
    def add_record(self, data_item):
        self.records.append(data_item)
    
    
        
    

    


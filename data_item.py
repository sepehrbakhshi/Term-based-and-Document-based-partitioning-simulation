# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 18:12:36 2022

@author: 3pehr
"""

class data_item:
    
    def __init__(self, term, doc_length):
        self.term = term
        self.doc_length = doc_length
        
    def set_term(self, term):
        self.term = term
        
    def set_doc_length(self, doc_length):
        self.doc_length = doc_length
    
    def get_term(self):
        return self.term

    def get_doc_length(self):
        return self.doc_length
    
        
    

    


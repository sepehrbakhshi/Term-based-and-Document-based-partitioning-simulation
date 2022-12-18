
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 18:12:36 2022

@author: 3pehr
"""
import numpy as np
from operator import attrgetter

class broker:
    
    def __init__(self):
        self.nodes = []
        self.all_terms = []
        self.length = []
        self.data_dict = []
    def set_node_items(self, k):
        for m in range(0, k):           
            data = self.nodes[m].records
            all_terms = [o.term for o in data]
            self.all_terms.append(all_terms)
            length = list(range(len(all_terms)))
            self.length.append(length)
            data_dict = dict(zip(all_terms, length))
            self.data_dict.append(data_dict)
    def get_node_items(self):
        return self.all_terms
    
    def set_nodes(self, nodes):
        self.nodes = nodes
 
    def get_nodes(self):
        return self.nodes
    
    def add_nodes(self, node):
        self.nodes.append(node)
    def get_size(self):
        return len(self.nodes)
    
    def search_for_nodes(self,query_words,term_based):
        qp_nodes = np.zeros((len(self.nodes), 1))
        qp_nodes_mins = np.zeros((len(self.nodes), 1))    
        qp_nodes_copy = np.zeros((len(self.nodes), 1))
        #all_terms = [o.term for o in data]      
        for term in query_words:
            # print(word)
            #broker.search_for_nodes(word)        
            for k in range(0, len(self.nodes)):
                data = self.nodes[k].records

                if term in self.data_dict[k].keys():
                    index = self.data_dict[k][term]
                    if(term_based):
                        if(qp_nodes[k]== 0 and qp_nodes_copy[k]== 0):
                            qp_nodes_copy[k] += data[index].doc_length
                        elif(qp_nodes[k] == 0 and qp_nodes_copy[k] != 0):
                            qp_nodes[k] = qp_nodes[k] + qp_nodes_copy[k] + data[index].doc_length
                            qp_nodes_copy[k] += data[index].doc_length
                        else:
                            qp_nodes[k] += data[index].doc_length
                    else:
                        qp_nodes[k] += data[index].doc_length
                    if(qp_nodes_mins[k] == 0):
                        qp_nodes_mins[k] += data[index].doc_length
                    elif(qp_nodes_mins[k] != 0):
                        qp_nodes_mins[k] = min(data[index].doc_length,qp_nodes_mins[k])              
        
        
        
        broker_cost = np.sum(qp_nodes_mins)
        overall_cost = np.max(qp_nodes) + broker_cost
        return qp_nodes, qp_nodes_mins, broker_cost, overall_cost
    
    
        
    

    


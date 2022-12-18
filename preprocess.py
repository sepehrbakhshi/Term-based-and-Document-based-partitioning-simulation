# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 11:23:09 2022

@author: 3pehr
"""
import numpy as np
from node import *
from data_item import *
from broker import *
import math

def preprocess():
    queries = open("10000_topics.txt", "r")
    queries_array = []
    count = 0
    
    for line in queries:
        queries_array.append(line)
    queries_array = np.array(queries_array)
    queries_only = []
    for k in range(0, len(queries_array)):
        a = queries_array[k]
        before, sep, after = a.partition(':')
        after = after.replace('\n', '')
        queries_only.append(after)
    queries_only = np.array(queries_only)
    
    wordlist = open("wordlist.txt", "r")
    wordlist_array = []
    for line in wordlist:
        wordlist_array.append(line)
    wordlist_array = np.array(wordlist_array)
    wordlist_array_2 = []
    words = []
    numbers = []
    for k in range(0, len(wordlist_array)):
        
        a = wordlist_array[k]
        
        before, sep, after = a.partition(' ')
        b = before
        words.append(before)
        before, sep, after = after.partition(' ')
        numbers.append(int(before))
        #after = a.replace('\n', '')
        #wordlist_array_2.append(after)
    words = np.array(words)
    words = words.reshape((words.shape[0],1))
    numbers = np.array(numbers)
    
    numbers = numbers.reshape((numbers.shape[0],1))
    wordlist_array_2 = np.concatenate((words, numbers), axis = 1)
    print(wordlist_array_2[0])
    #wordlist_array_2 = np.array(wordlist_array_2)
    
    return queries_only, wordlist_array_2
def round_robin_assignment_2(wordlist, limit, broker):
    
    node_index = 0
    #database = broker.get_nodes()
    space = np.zeros((broker.get_size(), 1))
    for term in wordlist:
        dt = data_item(term[0],int(term[1]))
        
        """
        if(node_index+1 < len(space) and space[node_index] > limit):
            space[node_index] = 0#space[node_index] - limit
            node_index += 1    
            
        elif(node_index+1 == len(space) and space[node_index] > limit):
            space[node_index] = 0#space[node_index] - limit
            node_index = 0
        
        """
        space[node_index] += dt.doc_length
        broker.nodes[node_index].add_record(dt)    
        node_index += 1
        if(node_index == broker.get_size()):
            node_index = 0
    
    return broker
def round_robin_assignment(wordlist, limit, broker):
    
    node_index = 0
    #database = broker.get_nodes()
    space = np.zeros((broker.get_size(), 1))
    for term in wordlist:
        
        dt = data_item(term[0],int(term[1]))
         
        if(node_index+1 < len(space) and space[node_index] > limit):
            space[node_index] = 0#space[node_index] - limit
            node_index += 1    
            
        elif(node_index+1 == len(space) and space[node_index] > limit):
            space[node_index] = 0#space[node_index] - limit
            node_index = 0
        
            
        space[node_index] += dt.doc_length
        broker.nodes[node_index].add_record(dt)    
        
    
    return broker

def document_based_partitioning(wordlist, broker):
    
    for term in wordlist:
        
        for i in range(0, broker.get_size()):
            pll_new = math.ceil((int(term[1])/broker.get_size()))
            dt = data_item(term[0],pll_new)
            broker.nodes[i].add_record(dt)    
           
            
    return broker
###################################################

queries, wordlist = preprocess()

#limit = 50
mode = input("Enter 1 for term-based and 2 for document-based partitioning: ")
mode = int(mode)
k = input("Enter your number of nodes (K): ")
k = int(k)
broker = broker()
for i in range(0, k):
    new_node = node()
    broker.add_nodes(new_node)


### term-based partitioning
limit = 1000
if(mode == 1):
    broker = round_robin_assignment_2( wordlist, limit,broker)

### doc-based partitioning
if(mode == 2):
    broker = document_based_partitioning(wordlist, broker)

#print total length of terms in each node
"""
a = 0
ll = []

for k in range(0, broker.get_size()):
    
    a = broker.nodes[k].records 
    length = 0
    for kk in range(0, len(a)):
        length += a[kk].doc_length
    ll.append(length)
print(np.sum(ll))
"""
#broker.search_for_nodes("zoo")
import time
count = 0
broker.set_node_items(k)
all_qp = np.zeros((k, 1))
qp_nodes = []
broker_cost_all = 0
overall_cost_all = 0
for query in queries:
    flag = False
    print(count)
    query_words = query.split()
    query_words = np.array(query_words)
    aaa = time.time()
    qp, qp_nodes_mins, broker_cost, overall_cost = broker.search_for_nodes(query_words,False)
    a = time.time() - aaa
    
    if(np.any(qp)):
        count += 1
    all_qp += qp
    broker_cost_all += broker_cost
    overall_cost_all += overall_cost
    print(a)
    print("**")
    print(qp)
    print(qp_nodes_mins)
    print(broker_cost)
    print(overall_cost)
    qp_nodes.append(qp)
    
    #break

all_qp_average = all_qp/count
broker_cost_all_average = broker_cost_all/count
print("Processed Query Coun: ", count)
print("Overall cost: ",overall_cost_all)
print("Average Overall Cost:",overall_cost_all/count)

#print(all_qp_average)
if(mode == 2):
    broker_cost_all_average = 0
print("Broker Cost: ", broker_cost_all_average)

print("*******")
print("Node Cost: ",all_qp)
print("Average Node Cost: ",all_qp_average)
#print(len(qp_nodes))

b_cost_all = np.zeros((1,1))
b_cost_all[0,0] = broker_cost_all
all_costs = np.concatenate((b_cost_all, all_qp))

import matplotlib.pyplot as plt
from scipy import interpolate

#b1 = batch_acc[:,40:100]
#maximums = []
"""
for i in range(0 , batch_acc.shape[1]):
    tmp = batch_acc[:,i:i+1]
    #print(tmp.max())
    maximums.append(tmp.max())
    print(np.mean(maximums))
"""
#plt.plot(b1.T , label = "OBLS")
#plt.plot(maximums , label = "OBLS")
plt.plot(all_costs ,marker ='*',label = "Node Cost")
#plt.plot(w_2_list[:,5] , label = "OBLS")
#plt.plot(w_2_list[:,100] , label = "OBLS")
if(k == 128):
    nodes_id = [0,10,20,30,40,50,60,70,80,90,100,110,120]
if(k == 4):
    nodes_id = [0,1,2,3,4]
if(k ==32):
    nodes_id = [0,5,10,15,20,25,30]
plt.xlabel('Nodes')
plt.ylabel('Cost')
plt.title("Node cost plot")
plt.xticks(nodes_id)
plt.grid()
plt.legend()
#plt.yticks(np.arange(0,100,10))
#plt.legend()
#plt.savefig('k=32_1_1000')
plt.show()



#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 11:42:56 2020

@author: Antoine.P
analyse.py
Using this program to collect and analyze data stored in MongoDB ( both of batch and speed layer)
"""

from datetime import datetime, timedelta
import pandas as pd
from collections import Counter
import pymongo


# make connection to MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient.tweets
mycolbatch = mydb.batch_test2
mycolstream = mydb.streaming_test2

class TweetAnalyse:
     """
     Attributes:
     dlist(list) A list of time under format YYYY-MM-DD-HH 
     nbtw(int) Top n most tweeted topics
     """
     def __init__(self, dlist, nbtw):
        self.dlist = dlist
        self.nbtw = nbtw
     
    
     def get_topics(self):
         dbatch = {}
         dstream = {}
         for it in self.dlist:
             
             mybatchquery = {"name": it}
             mybatchdocJson = mycolbatch.find_one(mybatchquery)
             try:
                # si le donnees existent sur le batch layer
                if mybatchdocJson is not None:
                    print("Batch layer: ")
                    d1 = dict(mybatchdocJson)['hashktags']
                    dmybatchdoc = {}
                    for i in range(len(d1)):
                        k = d1[i]['hashtags']
                        v = d1[i]['count']
                        dmybatchdoc[k] = v
                    dbatch.update(dmybatchdoc)
                    print("*******")
                    print("founded at %s" %it)
                # si non, c'est la speed layer qui prend le relais
                else:
                    print("========================================")
                    print("Streaming layer: ")
                    mystreamingquery = { "name": it}
                    mystreamingdocJson = mycolstream.find_one(mystreamingquery)
                    d2 = dict(mystreamingdocJson)['hashktags']
                    dmystreamingdoc = {}
                    for i in range(len(d2)):
                        k = d2[i]['hashtags']
                        v = d2[i]['count']
                        dmystreamingdoc[k] = v
                    dstream.update(dmystreamingdoc)
                    print("*******")
                    print("founded at %s" %it)
                    
             except:
                print("No data exist")
                print("at %s" %it)
         
         return dbatch,dstream 
     
     def get_result(self):
         try:
             print('Combining the results of the batch and the speed layer')
             D = dict(Counter(self.get_topics()[0]) + Counter(self.get_topics()[1]))
             Dsorted = sorted(D.items() , reverse=True, key=lambda x: x[1])
             # Iterate over the sorted sequence, keep the Top ten only
             print('')
             print("The %d most tweeted hashtags:\n" %self.nbtw)
             for elem in Dsorted[:self.nbtw] :
                 print(elem[0] , ":" , elem[1] )  
         except:
             print("No data exist, please select another time range")  
    
     # Using the @classmethod decorator to get input and supplies it to the __init__ function. 
     @classmethod
     def get_input(self):
            rp1 = None
            while rp1 is None:
                input_value = input("Voulez-vous recuperer les donnees depuis quelques dernieres heures? reponse Oui = O; Non = N ")
                try:

                    if str(input_value).lower() == 'o' or str(input_value).lower() == 'n':
                        rp1 = str(input_value)
                        print('ok')

                    else:
                        print("{input} Veuillez preciser si vous voulez recuperer les donnees depuis quelques dernieres heures? reponse Oui = O; Non = N".format(input=input_value))
                except ValueError:
                    print("{input} Veuillez preciser si vous voulez recuperer les donnees depuis quelques dernieres heures? reponse Oui = O; Non = N".format(input=input_value))
            
            if rp1 == 'o':
                print ("Depuis combien d'heures voulez-vous recuperer des donnees? example: 3")
                nbh = input()
                nbh = int(nbh)
                startdate = datetime.now() - timedelta(hours = nbh)
                enddate = datetime.now()
                print("date debut: %s" %startdate)
                print("date fin: %s" %enddate)
            else :  
                startdate = input("Veuillez renseigner une date debut sous format YYYY-MM-DD-HH (example 2020-07-27-15): ")
                year, month, day,hour = map(int, startdate.split('-'))
                startdate = datetime(year, month, day,hour)
                enddate = input("Veuillez renseigner une date de fin sous format YYYY-MM-DD-HH: ")
                year1, month1, day1,hour1 = map(int, enddate.split('-'))
                enddate = datetime(year1, month1, day1,hour1)
                print(startdate,enddate)

            time_range = pd.date_range(startdate, enddate, freq='H')    
            dlist = []
            for tmp in time_range:
                if len(str(tmp.hour)) > 1:
                    h = str(tmp.hour)
                else:
                    h = str(0) + str(tmp.hour)

                if len(str(tmp.day)) > 1:
                    d = str(tmp.day)
                else:
                    d = str(0) + str(tmp.day)
                if len(str(tmp.month)) > 1:
                    m = str(tmp.month)
                else:
                    m = str(0) + str(tmp.month)
                y = str(tmp.year)
                dlist.append(d+m+y+h)
            print(dlist)
            nbtw = input("Combien de sujets les plus tweetes que vous voulez trouver? ")
            nbtw = int(nbtw)
            return self(dlist,nbtw)

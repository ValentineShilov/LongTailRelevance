#Valentine Shilov
#relevance by user behaviour per-doc features for all queries
#initially started writing this script as a mapprer for hadoop streaming
#but then rewritten it to run on my pc, because it takes too long on hadoop (now only one hadoop node works)

import gzip
import bz2
import operator
from collections import defaultdict
from multiprocessing import Pool
import os
import numpy as np
import sys

import string
import re
import struct
import socket

from  urllib.parse import urlsplit
from  urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import quote
from urllib.parse import unquote
from posixpath import normpath
from Levenshtein import distance

rubrics_list = set(['health_consultations', 'spritze.deseasescmn', 'app', 'spritze.deseasesmsk', 'spritze.tv-programm', 'converter', 'answer', 'games', 'today', 'osmino', 'spritze.horoscope-sign', 'meta_video', 'weather', 'images', 'newstext', 'facts', 'map', 'spritze.metro', 'afisha', 'video', 'calendar', 'spritze.lady-treatment', 'spritze.lang', 'person', 'recipes', 'infocard.fact', 'howtos', 'spritze.realty-newb', 'drugs', 'music', 'news', 'spritze.dream-term', 'torgs', 'youla_web', "sport_cup", "spritze.realty-base", "NONE", "NULL", "promo_amigo"])
docToQuery = defaultdict(list)
qidToQuery = {}
MAX_SHOWS=20
def correctUrlList(l):
    global rubrics_list
    rv = []
    for url in l:
        if not ( url.startswith("http://") or  url.startswith("https://") or  url.startswith("ftp://") or url.startswith("httpm/") ):
            if (url in rubrics_list) or url.startswith("spritze."):
                rv.append(url)
            else:
                if len(rv)>=1:
                    rv[-1]+="," + url    
                else:
                    print("Error", url) 
                    rv.append(url)
        else:
            rv.append(url)
    return rv
def parseQueries(filename):
    global qidToQuery
    with open(filename, "r") as f:
        for line in f:
            qid,query = line.split("\t")
            query = query.strip()
            qid = int(qid)
            qidToQuery[qid] = query
def parseSample(filename):
    global docToQuery
    with open(filename, "r") as f:
        f.readline()
        for line in f:
            qid,did = line.split(",")
            qid,did = int(qid), int(did)
            docToQuery[did].append(qidToQuery[qid])

def url_normalize(url):
    global rubrics_list
    url = url.strip()

    url = url.replace("https://", "")
    url = url.replace("httpm//", "")
    url = url.replace("httpm/", "")
    url = url.replace("http://", "")
    url = url.replace("ftp://", "")
    if len(url)>1:
        if url[-1] == "/":
            url = url[:-1]
        url = url.lower()
        
        url = url.replace("s://", "")
        url = url.replace(":80", "")
        url = url.replace("://", "")
        url = url.replace("www.", "")
        url = url.replace("//", "")
        pos = url.find("#")
        if pos>0:
            url = url[0:pos]
            
    return url

class doc:
    def __init__(self):
        global MAX_SHOWS
        self.nclicks = 0
        self.nshows = 0
        self.pos_clicks = np.zeros(MAX_SHOWS, dtype=np.int)
        self.view_times = []
        self.nlast = 0
        self.nbefore=0
        self.nafter=0
        self.pos_done_clicks = np.zeros(MAX_SHOWS, dtype=np.int)
        self.pos_shows = np.zeros(MAX_SHOWS, dtype=np.int)
        self.url = ""
        self.avg_time_cnt=0
        self.avg_time = 0
        self.ld=100000000
        self.ldc=100000000

    def update_lev(self, query):
        global docToQuery
        global urls
        if self.url in urls:
            did = int(urls[self.url])
            doc_queries = docToQuery[did]
            for q in doc_queries:
                ld = distance(query, q)
                self.ld = min(self.ld, ld)
    def update_lev_clicked(self, query):
        global docToQuery
        global urls
        if self.url in urls:
            
            did = int(urls[self.url])
            
            doc_queries = docToQuery[did]

            for q in doc_queries:
                #print("sfafdafsf")
                ld = distance(query, q)
                #print(ld)
                self.ldc = min(self.ldc, ld)
    def __str__(self):
        a = np.nonzero(self.pos_clicks)[0]
        if(a.shape[0]==0):
            a = np.zeros(1, dtype=np.int)
        b = self.pos_clicks[0:np.max(a)+1]
        if len(self.view_times)>0:
            self.avg_time = np.mean(np.array(self.view_times))
        else: 
            #avg_time = 0
            pass
        avg_time = self.avg_time
        aa = np.nonzero(self.pos_done_clicks)[0]
        if(aa.shape[0]==0): 
            aa = np.zeros(1, dtype=np.int)
        bb = self.pos_done_clicks[0:np.max(aa)+1]

        aaa = np.nonzero(self.pos_shows)[0]
        if(aaa.shape[0]==0): 
            aaa = np.zeros(1, dtype=np.int)
        bbb = self.pos_shows[0:np.max(aaa)+1]
        
        rv = self.url + "\t" + str(self.nclicks) + "\t" + str(self.nshows) + "\t" + str(self.nlast) + "\t" + str(avg_time) + "\t"+ str(b).replace("[", "").replace("]", "").replace("\t", " ").strip() + "\t" + str(bb).replace("[", "").replace("]", "").replace("\t", " ").strip()  + "\t" + str(self.ld)  + "\t" + str(self.ldc)+ "\t" + str(self.nbefore) + "\t" + str(self.nafter) + "\t" + str(bbb).replace("[", "").replace("]", "").replace("\t", " ").strip()
        return rv.replace("\n", "")

    def __repr__(self):
        return self.__str__()
#docs = defaultdict(doc)
urls= {}
def loadUrls(filename):
    global urls
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            uid, url = line.split("\t")

            urls[url_normalize(url)] = uid
DIRPREFIX = "data/2017/"
RES_PREFIX = "res15/"
rubrics = set()
def processFile(filename):
    try:
        #global docs
        global DIRPREFIX
        global urls
        global rubrics
        global MAX_SHOWS
        global RES_PREFIX

        docs = defaultdict(doc)
        #print(DIRPREFIX, filename)
        #f = bz2.open(DIRPREFIX + filename,'r')
        f = open(DIRPREFIX + filename,'r')
        cnt=0
        for linenum,line in enumerate(f):
            try:
                #line = line.decode("utf-8", errors="ignore")

                line = line.replace("\n", "")
                parts = line.split("\t")
                if(len(parts)>3):
                    q_reg = parts[0]
                    query, reg = q_reg.split("@")
                    shown__urls = correctUrlList(parts[1].split(","))

                    shown__urls = list(map(url_normalize, shown__urls))
                    
                    
                    clicked_urls = correctUrlList(parts[2].split(","))
 

                    clicked_urls =  list(map(url_normalize, clicked_urls))

                    time_clicks = parts[3].split(",")
                    url_time = list(zip(clicked_urls, time_clicks))
                    if len(clicked_urls)<=1:

                        pass
                    if len(shown__urls)>=MAX_SHOWS:
                        continue
                    sorted_url_time = sorted(url_time, key=operator.itemgetter(1))
                    sorted_url_time_len = len(sorted_url_time)
                    for i, (url,time) in enumerate(sorted_url_time):
                        
                        if url in urls:
                            
                            if i == sorted_url_time_len - 1:
                                docs[url].nlast += 1
                            elif i>0:
                                 docs[url].view_times.append(int(time) - int(prev_time))
                            docs[url].pos_done_clicks[i]+=1
                            docs[url].nbefore+=i
                            docs[url].nafter+=len(sorted_url_time)-i-1
                        prev_time=time
                    for i, url in enumerate(clicked_urls): #in enumerate(sorted_url_time):
                        
                        if url not in urls:
                            continue
                            pass
                        docs[url].nclicks+=1
                        docs[url].url = url
                        docs[url].nshows+=1
                        try:
                            docs[url].pos_clicks[shown__urls.index(url)]+=1
                        except Exception as e:
                            print(line, repr(e))
                        docs[url].update_lev_clicked(query)
                    for i,url in enumerate(shown__urls):
                        
                        if url not in urls:
                            continue
                            pass
                        docs[url].nshows+=1
                        docs[url].url = url
                        docs[url].update_lev(query)
                        try:
                            docs[url].pos_shows[shown__urls.index(url)]+=1
                        except Exception as e:
                            print(line, repr(e))

            except Exception as e:
                print(filename, linenum,  e, flush=True)    
        
      
            #break
        with open(RES_PREFIX + filename + ".txt", "w") as g:
            for url,d in docs.items():
                g.write(str(urls[url]) + "\t" + str(d) + "\n")
                cnt+=1
        print("Processed "+ filename +" " + str(cnt), flush=True)
        return cnt
    except Exception as e:
        print("Error ", filename,  repr(e), flush=True)    

if __name__ == '__main__':
    if len(sys.argv)>=4:
        RES_PREFIX = sys.argv[2]
        DIRPREFIX = sys.argv[1]
        DATA_PREFIX = sys.argv[3]
    print(RES_PREFIX, DIRPREFIX, DATA_PREFIX)
    loadUrls(DATA_PREFIX + "url.data")
    parseQueries(DATA_PREFIX + "queries.tsv")
    parseSample(DATA_PREFIX + "sample.csv")
    files = list( os.listdir(DIRPREFIX))

    with Pool(4) as p:
        p.map(processFile, files)
        
      

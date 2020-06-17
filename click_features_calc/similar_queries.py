import gzip
import bz2
import operator
from collections import defaultdict
from multiprocessing import Pool
import os
import numpy as np


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
docToQids = defaultdict(list)
minDistances = {}

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
def parseQueries2(filename):
    global qidToQuery
    with open(filename, "r") as f:
        for line in f:
            qid,query = line.split("\t")
            query = query.strip()
            qid = int(qid)
            qid+=1000000000
            qidToQuery[qid] = query
def parseSample(filename):
    global docToQuery
    global docToQids
    with open(filename, "r") as f:
        f.readline()
        for line in f:
            qid,did = line.split(",")
            qid,did = int(qid), int(did)
            docToQuery[did].append(qidToQuery[qid])
            docToQids[did].append(qid)
            if (qid+1000000000) in qidToQuery:
            	docToQids[did].append(qid+1000000000)

def url_normalize(url):
    global rubrics_list
    url = url.strip()
    #for r in rubrics_list:
        #url = url.replace("," + r + "," , "")    
    #    url = url.replace("," + r, "")    
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
urls= {}
def loadUrls(filename):
    global urls
    with open("url.data", "r") as f:
        for line in f:
            line = line.strip()
            uid, url = line.split("\t")
            uid = int(uid)
            #urls["http://" + url] = uid
            #urls["http://" + url + "/"] = uid
            urls[url_normalize(url)] = uid
DIRPREFIX = "../data/2017/"


def processFile(filenum,filename):
    try:
        #global docs
        global DIRPREFIX
        global urls
        global rubrics
        global MAX_SHOWS
        global minDistances
        #docs = defaultdict(doc)
        #print(DIRPREFIX, filename)
        f = bz2.open(DIRPREFIX + filename,'r')
        
        cnt=0
        for linenum,line in enumerate(f):
            try:
                line = line.decode("utf-8", errors="ignore")
                #if line[-1]=="\n":                
                #    line = line[:-1]
                line = line.replace("\n", "")
                parts = line.split("\t")
                if(len(parts)>3):
                    q_reg = parts[0]
                    query, reg = q_reg.split("@")
                    shown__urls = correctUrlList(parts[1].split(","))
                    shown__urls = list(map(url_normalize, shown__urls))
                    shown__urls = list(filter(lambda x: x in urls, shown__urls))
                    shown__dids = list(map(lambda x: urls[x], shown__urls))
                    #print(shown__dids)
                    qids = set()
                    for did in  shown__dids:
                    	for tmp in docToQids[did]:
                    		qids.add(tmp)
                    	#qids+=
                    	cnt+=1
                    #print(qids)
                    for qid in qids:
                    	cur_query = qidToQuery[qid]

                    	if qid in minDistances:
                    		prev_dist,data = minDistances[qid]
                    	else:
                    		prev_dist=1000000000
                    	cur_dist = distance(query, cur_query)
                    	if cur_dist==prev_dist:
                    		if len(data)<1500:
                    			minDistances[qid] = (cur_dist, data + [line])
                    	elif cur_dist<prev_dist:
                    		minDistances[qid] = (cur_dist, [line])
            except Exception as e:
                	print(filename, linenum,  e, flush=True)    
        
        print(filenum, filename, len(minDistances), cnt, flush=True)
    except Exception as e:
        print("Error ", filename,  repr(e), flush=True)       


loadUrls("url.data")
parseQueries("queries.tsv")
parseQueries2("fspell.txt")
parseSample("sample.csv")
files = list( os.listdir("../data/2017/"))
#processFile(1000,"../cq/1000cq.txt.bz2")
for i,file in enumerate(files):
	if i<=1000:
		continue
	processFile(i,file)

with open("cq.txt", "w") as f:
	for qid, (dist,data) in minDistances.items():
		for dt in data:
			f.write(dt + "\n")

with open("cqe.txt", "w") as f:
	for qid, (dist,data) in minDistances.items():
		for dt in data:
			f.write(str(qid) +"\t" + str(dist) + "\t" + dt + "\n")

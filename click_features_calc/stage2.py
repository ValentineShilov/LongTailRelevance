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
MAX_SHOWS=20
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
        self.url = ""
        self.avg_time_cnt=0
        self.avg_time = 0
        self.ld=100000000
        self.ldc=100000000
        self.pos_shows = np.zeros(MAX_SHOWS, dtype=np.int)
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
               
                ld = distance(query, q)
                
                self.ldc = min(self.ldc, ld)
    def __str__(self):
        a = np.nonzero(self.pos_clicks)[0]
        if(a.shape[0]==0):
            a = np.zeros(1, dtype=np.int)
        b = self.pos_clicks[0:np.max(a)+1]
        if len(self.view_times)>0:
            self.avg_time = np.mean(np.array(self.view_times))
        else: 
            
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
    def agr_from_str(self, line):
        parts = line.split("\t")
        if len(parts)<=10:
            print("error", len(parts), parts)
            return
        #print(len(parts), line)
        self.nclicks += int(parts[1])
        self.nshows += int(parts[2])
        self.nlast += int(parts[3])
        self.url = parts[0]
        at = float(parts[4])
        if self.avg_time_cnt==0 and at!=0:
            self.avg_time = at
            self.avg_time_cnt+=1
        elif at!=0:
            self.avg_time_cnt+=1
            self.avg_time =  (self.avg_time*(self.avg_time_cnt-1) + at) / (self.avg_time_cnt)
        pks = parts[5].split(" ")
        pks = list(filter(lambda x: len(x)>0, pks))
        #print(pks)
        pk = list(map(int, pks))
        
        pk = np.array(pk)
        self.pos_clicks[0:len(pk)] += pk

       
        ppks = parts[6].split(" ")
        ppks = list(filter(lambda x: len(x)>0, ppks))
        ppk = list(map(int, ppks))
        
        ppk = np.array(ppk)
        self.pos_done_clicks[0:len(ppk)] += ppk
        
        self.ld=min(int(parts[7]), self.ld)
        self.ldc=min(int(parts[8]), self.ldc)
        self.nbefore+=int(parts[9])
        self.nafter+=int(parts[10])



        pppks = parts[11].split(" ")
        pppks = list(filter(lambda x: len(x)>0, pppks))
        pppk = list(map(int, pppks))
        
        pppk = np.array(pppk)
        self.pos_shows[0:len(pppk)] += pppk
      
urls= {}
def loadUrls(filename):
    global urls
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            uid, url = line.split("\t")
            urls["http://" + url] = uid
            urls["http://" + url + "/"] = uid
DIRPREFIX = "../data/2017/"
RESPREFIX = "../res15/"
DATA_PREFIX = "code/"
alldocs = defaultdict(doc)
def processFile(filename):
    try:
        #global docs
        global RESPREFIX
        global urls
        global alldocs
        #print(DIRPREFIX, filename)
        f = open(RESPREFIX + filename,'r')
        
        cnt=0
        for linenum,line in enumerate(f):
            try:
                line = line[:-1]
                parts = line.split("\t", maxsplit=1)
                #print(parts)
                uid = parts[0]
                #print(linenum)
                alldocs[int(uid)].agr_from_str(parts[1])
            except Exception as e:
                print("Error ", filename,  repr(e), linenum)    
        print("Processed "+ filename +" " + str(cnt))
        cnt+=1
        return cnt
    except Exception as e:
        print("Error ", filename,  repr(e))    

if __name__ == '__main__':
    OUT_FILE = "features_15.txt"
    if len(sys.argv)>=5:
        RESPREFIX = sys.argv[2]
        DIRPREFIX = sys.argv[1]
        DATA_PREFIX = sys.argv[3]
        OUT_FILE = sys.argv[4]
        
    print(RESPREFIX, DIRPREFIX, DATA_PREFIX)
    loadUrls(DATA_PREFIX + "url.data")
    files = list( os.listdir(RESPREFIX))
    print(files)
    #files = map(lambda x: "../data/2017/" + x ,files)
    #processFile("part-m-00094.bz2.txt")
    
    #with Pool(4) as p:
    #    p.map(processFile, files)
    if(True):
        cnt = 0
        for filename in files:
            processFile(filename)
            cnt+=1
            if(cnt%100==0):
                #break;
                print(cnt, filename, len(alldocs))
                
        cnt = 0
        with open(DATA_PREFIX + "features_15.txt", "w") as g:
            for did,d in alldocs.items():
                g.write(str(did) + "\t" + str(d) + "\n")
                cnt+=1
                if(cnt%50==0):
                    print(cnt, did)
            
           
       
        

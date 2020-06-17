#!/usr/bin/env python
# coding: utf-8

# In[3]:





# In[1]:


from multiprocessing import Pool, Queue, Process
import pymorphy2
N_CORES=4


# In[2]:


content = Queue(maxsize=200)
log = Queue(maxsize=1000)
def getLines(nlines, content):
    with open("read_log.txt", "w") as g:
        with open("../data/docs.tsv") as f:
            for nline,line in enumerate(f):
                content.put( line)
                if (nline>nlines) and (nlines>0):
                    break
                if(nline%1000==0):
                    g.write( "read:" + str( nline))


# In[3]:


fileReader = Process(target = getLines, args = (-300,content))
fileReader.start()
        


# In[4]:





# In[ ]:





# In[5]:


from functools import reduce
from functools import lru_cache
@lru_cache(maxsize=5000)
def normWord(morph, word):
    
    p = morph.parse(word)
    if len(p)>0:
        norm = p[0].normal_form
    else:
        norm = word
    return norm
    
def docWorker(wid, q):
    morph = pymorphy2.MorphAnalyzer()
    with open("log" + str(wid), "w") as log:
        with open("out" + str(wid) + ".txt", "w") as g:
            with open("titles_only" + str(wid) + ".txt", "w") as h:
                it = 0
                while(True):
                    line = q.get(block=True, timeout=100)
                    line = line.strip()
                    parts = line.split("\t")
                    if(len(parts)>2):
                        did = int(parts[0])
                        tittle = parts[1]
                        doc = parts[2]
                        tittle = tittle.lower()
                        twords = tittle.split(" ")
                        twords = list(filter(lambda x: len(x)>2 , twords))
                        twords = list(map(lambda x: normWord(morph,x) , twords))
                        dwords = doc.split(" ")
                        dwords = list(filter(lambda x: len(x)>2 , dwords))
                        dwords = list(map(lambda x: normWord(morph,x) , dwords))
                        if len(dwords)>0:
                            dwords = str(reduce(lambda x,y: x +" " + y , dwords))
                        else:
                            dwords = ""
                        if len(twords)>0:
                            twords = str(reduce(lambda x,y: x +" " + y , twords))
                        else:
                            twords=""
                        g.write(str(did) + "\t" + twords +"\t" + dwords +"\n")
                        h.write(str(did) + "\t" + twords + "\n")
                        if (it%1000==0):
                            log.write("Worker " + str(wid) +" read " + str(it) + "\n")
                        it+=1


# In[6]:


#log.get(


# In[7]:





# In[8]:





# In[9]:


processes = []
for i in range(N_CORES):
    processes.append( Process(target = docWorker, args = (i,content)))


# In[10]:


for p in processes:
    p.start()


# In[ ]:


for p in processes:
    p.join()


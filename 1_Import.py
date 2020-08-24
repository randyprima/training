# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 09:21:44 2020

@author: randy
"""
import os, sys
import tqdm

def runApp(app,tableNm,hostname,metadata,database,username,password,dirtem,metadata,tilesize):
    os.popen(app+' --tbl '+tableNm+' --hos '+hostname+'  --dbn '+database+' --usr '+username+' --psw '+password+' --dir '+dirtem+' --met '+metadata+' --tzs '+tilesize)
    
def main():
    listFiles = r'..\files\list.txt' #sys.argv[1]
    tableNm = sys.argv[1] #r'testSPOT' #sys.argv[2]
    dirtem = r'..\temporal' #sys.argv[3]
    hostname = 'localhost'
    database = 'test'
    username = 'administrator'
    password = 'teklahta@8'
    tilesize = '0.1'
    if not os.path.isdir(dirtem): 
        os.mkdir(dirtem)
    app = r'python calculateCloud_FromPolygon.py '
    ins = []
    with open(listFiles, 'r') as filein:
        lines = filein.readlines()
        for line in lines:
            metadata = line.strip()
            ins.append((app,tableNm,hostname,metadata,database,username,password,dirtem,metadata,tilesize),)
            
    
    for i in tqdm.tqdm(range(0,len(ins)),desc='Progress'):
        runApp(ins[i])
    
if __name__ == '__main__':
    main()
    

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 09:21:44 2020

@author: randy
"""
import os, sys
from multiprocessing import Pool
# import tqdm

def multi_run_wrapper(args):
   return runApp(*args)

def runApp(app,tableNm,dirTm,metadata):
    os.popen(app+' '+tableNm+' '+dirTm+' '+metadata)
    
def main():
    listFiles = r'..\files\list.txt' #sys.argv[1]
    tableNm = sys.argv[1] #r'testSPOT' #sys.argv[2]
    dirTm = r'..\temporal' #sys.argv[3]
    if not os.path.isdir(dirTm): 
        os.mkdir(dirTm)
    app = r'python calculateCloud_FromPolygon.py '
    ins = []
    with open(listFiles, 'r') as filein:
        lines = filein.readlines()
        for line in lines:
            ins.append((app,tableNm,dirTm,line.strip()),)
            
    pool = Pool(1)
    pool.map(multi_run_wrapper, ins)
    pool.close()
    # for _ in tqdm.tqdm(pool.imap_unordered(multi_run_wrapper, ins), total=len(ins)):
        # pass
    
if __name__ == '__main__':
    main()
    
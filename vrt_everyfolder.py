# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 14:57:50 2019

@author: pustekdata-2
"""

import os, sys
from multiprocessing import Pool

def main(pathfolder):
    folder = os.path.basename(pathfolder)
    vrt  = pathfolder+os.sep+folder+'_merged.vrt'
    os.system("gdalbuildvrt "+vrt+' '+pathfolder+os.sep+'*.tif')
    

if __name__ == "__main__":
    diri=sys.argv[1] 	#r"E:\randy\SPOT6-7\HASIL_COLOR_BALANCING_SPOT67_TILE-BASED_2017"
    pathfolders=[]
    for r,d,f in os.walk(diri, topdown=False):
       for folder in d:
            pathfolder = os.path.join(r,folder)            
            pathfolders.append(pathfolder)
            
    pool = Pool(4);	pool.map(main, pathfolders); pool.close(); pool.join()
            
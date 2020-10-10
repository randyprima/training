# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 14:57:50 2019

@author: pustekdata-2
"""

import os, fnmatch, sys

if __name__ == "__main__":
    diri=sys.argv[1]
    
    file_filter = '*.tif' 
    alltif={}
    
    for r,d,f in os.walk(diri, topdown=False):
        for nama_tile in fnmatch.filter(f, file_filter):
            fullpath=os.path.join(r,nama_tile)            
            namas = nama_tile.split('_')
            lat = namas[2]
            lon = namas[3].rstrip('.tif')
            new_lat = lat[:-2]+'0'+lat[-1]
            new_lon = lon[:-2]+'0'+lon[-1]
            
            folder=new_lat+'_'+new_lon
            
            dst = diri+os.sep+folder
            
            if not os.path.isdir(dst):
                os.mkdir(dst)
                print("moving "+nama_tile+" to "+dst)
                os.rename(fullpath,os.path.join(dst,nama_tile))
            else:
                print("moving "+nama_tile+" to "+dst)
                os.rename(fullpath,os.path.join(dst,nama_tile))
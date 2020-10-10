# -*- coding: utf-8 -*-
"""
Created on Tue Jun 25 12:47:42 2019

@author: pustekdata-2
"""

import os, sys, fnmatch
from multiprocessing import Pool

def multi_run_wrapper(args):
   return main(*args)
   
def main(fili,filo):
    os.system("convert_to_RGB.py "+fili+" "+filo)
    
        
    
if __name__ == "__main__":
    diri=sys.argv[1]
    file_filter='*.vrt'
    filis=[]
    for r,d,f in os.walk(diri, topdown=False):
       for fili in fnmatch.filter(f, file_filter):
            input = os.path.join(r,fili)
            output = input.replace('vrt','tif')
            filis.append((input,output),)
			#print(fili)
			#main(os.path.join(r,fili))
    pool = Pool(4);	pool.map(multi_run_wrapper, filis); pool.close(); pool.join()
            
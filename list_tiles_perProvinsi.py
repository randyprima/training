# -*- coding: utf-8 -*-
"""
Created on Mon Dec  9 16:34:00 2019

@author: Randy
"""
from osgeo import gdal, ogr
gdal.PushErrorHandler('CPLQuietErrorHandler')
gdal.UseExceptions()

import os, sys

index=sys.argv[1]   #r'C:\Users\Randy\Documents\2019\mosaik_index\2019\mosaik-tile_index2019_fixed_withce90.shp'
batas=sys.argv[2]   #r'E:\Batas_admin\Admin_PROV\PROV_AR.shp'

#membuka file roi untuk mendapatkan batas berbentuk persegi empat
driver = ogr.GetDriverByName('ESRI Shapefile')
shp1 = driver.Open(batas, 0) # 0 means read-only. 1 means writeable.

layer1 = shp1.GetLayer(0)

for feature1 in layer1:
    geom1 = feature1.GetGeometryRef()
    namabatas=feature1.GetField("WA")
    teks = namabatas+'.txt'
    if not os.path.isfile(teks):
        o=open(namabatas+'.txt','wt')
        driver = ogr.GetDriverByName('ESRI Shapefile')
        shp2 = driver.Open(index, 0) # 0 means read-only. 1 means writeable.
        layer2 = shp2.GetLayer(0)
        for feature2 in layer2:
            geom2 = feature2.GetGeometryRef()  
            intersection = geom1.Intersection(geom2)
            env = intersection.GetEnvelope() #env = (104.90154728287335, 105.20599799715906, -5.414043431412336, -5.208592424918832)
            
            if not env[0]+env[3]+env[1]+env[2]==0:
                #print(env[0],env[3],env[1],env[2])            
                namatile=feature2.GetField("Nama_Tile")
                print(namabatas,namatile)
                o.write(namatile+'\n')
        o.close() 

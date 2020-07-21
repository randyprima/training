# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 01:25:20 2020

@author: randy
"""

from __future__ import division
import os, math, sys
# import getpass
import psycopg2
from psycopg2 import sql
# import tqdm
from osgeo import ogr, gdal, osr
import numpy as np
# from multiprocessing import Pool

def postgres_test(hostname,db_name, username, pascode):
    try:
        conn = psycopg2.connect(host=hostname, database=db_name, user=username, password=pascode, connect_timeout=1)
        conn.close()
        return True
    except:
        return False

def multi_run_wrapper2(args):
   return ingest(*args)

def ingest(hostname,db_name,username,db_table,pascode,short_name,yy_start,xx_start,tilesize,ul_latitude,ul_longitude,result_raster,smetadata,incl,incx,date,geometric,spectral,rmse,rndval):
    conn = psycopg2.connect(host=hostname, database=db_name, user=username, password=pascode)
    c = conn.cursor()
    lr_latitude=round(ul_latitude-tilesize,rndval)
    lr_longitude=round(ul_longitude+tilesize,rndval)
    if str(ul_latitude)[0]=="-":
        lat=str(ul_latitude).replace("-","S").replace(".","")
    else:
        lat='N'+str(ul_latitude).replace(".","")
    
    if len (lat) >= 5:
        new_lat = lat[:1]+'0'+lat[1:]
    elif len(lat) == 4:
        new_lat = lat[:1]+'00'+lat[1:]
    elif len(lat) == 3:
        new_lat = lat[:1]+'000'+lat[1:]
    else:
        new_lat = lat
    
    lon = 'E'+str(ul_longitude).replace(".","")
    if len (lon) >= 5:
        new_lon = lon[:1]+'0'+lon[1:]
    else:
        new_lon = lon
        
    nama_tile=new_lat+"_"+new_lon
    
    res_raster_clip = result_raster.replace('.tif','_'+nama_tile+'_clip.vrt')
        
    raster = gdal.Warp(res_raster_clip, result_raster, dstSRS='EPSG:4326', format='VRT', outputBounds=(ul_longitude,ul_latitude,lr_longitude,lr_latitude))
    
    banddataraster = raster.GetRasterBand(1)
    dataraster = banddataraster.ReadAsArray().astype(np.uint8)

    # nodata = (dataraster==1).sum()
    clear = (dataraster==2).sum()
    cloud = (dataraster==101).sum()
    pix_count = np.size(dataraster)
    ndata = round((((clear+cloud)/pix_count)*100),2)
    #nclear = round(clear/pix_count,2)
    ncloud = round(cloud/pix_count*100,2)
    raster = None
    os.remove(res_raster_clip)
    c.execute(sql.SQL("INSERT INTO {} (sceneid, ullon_scene, ullat_scene, tile_size, ul_latitude, ul_longitude, ndata, pixdata, ncloud, metadata, inc_along, inc_across, img_date, geometric, spectral, ce90) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").format(sql.Identifier(db_table)), (short_name,yy_start,xx_start,tilesize,ul_latitude,ul_longitude,ndata,pix_count,ncloud,metadata,incl,incx,date,geometric,spectral,rmse))
    conn.commit()
    conn.close()

def main (hostname, db_name, username, metadata, db_table, pascode, dirtemp, tilesize, rndval):
    smetadata = os.path.basename(metadata) # DIM_SPOT6_PMS_201809040237336_ORT_SPOT6_20180904_02503513ik99j3p4hk8_1.XML    
    roi = os.path.dirname(metadata)+os.sep+'MASKS'+os.sep+smetadata.replace('DIM','ROI').replace('.XML', '_MSK.GML')
    cld = roi.replace('ROI','CLD')
    if (os.path.isfile(metadata) and os.path.isfile(cld)) == True:
        pmetadata = smetadata.split('_')
        short_name = pmetadata[1]+'_'+pmetadata[2]+'_'+pmetadata[3]
        
        # baca database
        c.execute(sql.SQL("SELECT * FROM {} WHERE sceneid =%s").format(sql.Identifier(db_table)), (short_name,))
        hs = c.fetchall()
        if hs == []:
        
            incl=[];incx=[];ce90=[]
            with open(metadata) as filein:
            	for line1 in filein:
            		baris = line1.strip()
            		if baris.find("<INCIDENCE_ANGLE_ALONG_TRACK>")==0:
            			incl.append(baris.split(">")[1].split("<")[0])
            		if baris.find("<INCIDENCE_ANGLE_ACROSS_TRACK>")==0:
            			incx.append(baris.split(">")[1].split("<")[0])
            		if baris.find("<IMAGING_DATE>")==0:
            			date=baris.split(">")[1].split("<")[0]
            		if baris.find("<GEOMETRIC_PROCESSING>")==0:
            			geometric=baris.split(">")[1].split("<")[0]
            		if baris.find("<SPECTRAL_PROCESSING>")==0:
            			spectral=baris.split(">")[1].split("<")[0]
            		if baris.find("<ACCURACY_CE90")==0:
            			ce90.append(baris.split(">")[1].split("<")[0])
            	if len(ce90)>0:
            		rmse=str(round(float(ce90[0])*30.87,2))
            		if rmse=='0' or rmse=='0.0':
            			rmse='N/A'
            	else:
            		rmse='N/A'
            
            driver = ogr.GetDriverByName("GML")
            dataSource = driver.Open(roi, 0)
            layer = dataSource.GetLayer()
            for feature in layer:
                geom = feature.GetGeometryRef()
                env = geom.GetEnvelope()
            roi_raster = dirtemp+os.sep+os.path.basename(roi)[:28]+".tif"
            os.popen("gdal_rasterize -q -a_srs EPSG:4326 -l MaskFeature -burn 1 -tr 0.0005 0.0005 -init 0 -te %f %f %f %f -ot Byte -of GTiff %s %s" %(env[0],env[2],env[1],env[3],roi,roi_raster))
            
            cld_raster = dirtemp+os.sep+os.path.basename(cld)[:28]+".tif"  
            os.system("gdal_rasterize -q -a_srs EPSG:4326 -l MaskFeature -burn 100.0 -tr 0.0005 0.0005 -init 1 -te %f %f %f %f -ot Byte -of GTiff %s %s" %(env[0],env[2],env[1],env[3],cld,cld_raster))
            
            result_raster = cld_raster.replace("CLD","RES")
            if os.path.isfile(roi_raster) and os.path.isfile(cld_raster):
                #open the dataset
                ds1 = gdal.Open(roi_raster)
                geotransform = ds1.GetGeoTransform()
                originX = geotransform[0]
                originY = geotransform[3]
                pixelWidth = geotransform[1]
                pixelHeight = geotransform[5]
                ds1band = ds1.GetRasterBand(1)
                ds1array = ds1band.ReadAsArray().astype(np.uint8)
                
                ds2 = gdal.Open(cld_raster)
                ds2band = ds2.GetRasterBand(1)
                ds2array = ds2band.ReadAsArray().astype(np.uint8)
                
                result_array = ds1array+ds2array
                
                driver = gdal.GetDriverByName('GTiff')
                outRaster = driver.Create(result_raster, ds1.RasterXSize, ds1.RasterYSize, 1, gdal.GDT_Byte)
                outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
                outband = outRaster.GetRasterBand(1)
                outband.WriteArray(result_array)
                outRasterSRS = osr.SpatialReference()
                outRasterSRS.ImportFromWkt(ds1.GetProjectionRef())
                outRaster.SetProjection(outRasterSRS.ExportToWkt())
                outband.FlushCache()
                
                ds1=None; ds2=None; outRaster=None
                # os.remove(cld_raster)
                # os.remove(roi_raster)
            
                if os.path.isfile(result_raster):
                    
                    yy_start = round((env[2]//tilesize)*tilesize,rndval)+tilesize
                    yy_end = round((math.ceil(env[3]//tilesize))*tilesize,rndval)+2*tilesize
                    xx_start = round((env[0]//tilesize)*tilesize,rndval)
                    xx_end = round((env[1]//tilesize)*tilesize,rndval)+tilesize
                    
                    yy = np.arange(yy_start,yy_end,tilesize)
                    xx = np.arange(xx_start,xx_end,tilesize)
                    
                    for y in yy:
                        for x in xx:
                            ul_longitude=round(x,rndval)
                            ul_latitude=round(y,rndval)
                                        
                            #baca database
                            c.execute(sql.SQL("SELECT * FROM {} WHERE (sceneid =%s AND ul_latitude=%s AND ul_longitude=%s)").format(sql.Identifier(db_table)), (short_name,ul_latitude,ul_longitude))
                            hs = c.fetchall()
                            if hs == []: #belum ada di database, jadi bisa insert data baru di database.
                                if short_name[:4] == 'PHR1':
                                    ingest(hostname,db_name,username,db_table,pascode,short_name,yy_start,xx_start,tilesize,ul_latitude,ul_longitude,result_raster,smetadata,incl[1],incx[1],date,geometric,spectral,rmse,rndval)
                                elif short_name[:4] == 'SPOT':
                                    ingest(hostname,db_name,username,db_table,pascode,short_name,yy_start,xx_start,tilesize,ul_latitude,ul_longitude,result_raster,smetadata,incl[4],incx[4],date,geometric,spectral,rmse,rndval)
                    conn.close()
                    
    else:
        print('file %s tidak lengkap' %(smetadata))
        pass


if __name__ == '__main__':
    db_table = sys.argv[1] #'testPleiades' #sys.argv[4] #'test_db'
    hostname = '192.168.40.34' #sys.argv[5]
    db_name = 'test' #sys.argv[6]
    username = 'administrator' #sys.argv[7]
    pascode = 'teklahta@8' #getpass.getpass()
    dirtemp = sys.argv[2] #r'D:\temp\Pleiades'
    metadata = sys.argv[3] #r"D:\temp\Pleiades\LPN_PHR1B_PMS_201812050255065_ORT\DIM_PHR1B_PMS_201812050255065_ORT_PHR1B_20181205_0348231mis19470fhzz_1.XML"
    tilesize = 0.1
    rndval=len(str(tilesize).split('.')[-1]) #jumlah angka atau digit di belelakang koma atau titik (nilai desimal)
    
    
    if postgres_test(hostname,db_name, username, pascode): 
        conn = psycopg2.connect(host=hostname, database=db_name, user=username, password=pascode)
        c = conn.cursor()
        c.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (sceneid TEXT, ullon_scene numeric(3), ullat_scene numeric(3), tile_size numeric(4,3), ul_latitude numeric(6,2), ul_longitude numeric(6,2), ndata numeric(6,2), pixdata numeric(6), ncloud numeric(6,2), metadata TEXT, inc_along numeric(7,3), inc_across numeric(7,3), img_date DATE, geometric TEXT, spectral TEXT, ce90 TEXT)") .format(sql.Identifier(db_table)))
        conn.commit()
        
        main (hostname, db_name, username, metadata, db_table, pascode, dirtemp, tilesize, rndval)
        conn.close()
    else:
        print('authentication failure. Please check your host, database_name, user and password')
    
    
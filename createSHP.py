# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 01:25:20 2020

@author: randy
"""
from __future__ import division
import os
# import getpass
import psycopg2
from psycopg2 import sql
# import tqdm
from osgeo import ogr

def postgres_test(hostname,db_name, username, pascode):
    try:
        conn = psycopg2.connect(host=hostname, database=db_name, user=username, password=pascode, connect_timeout=1)
        conn.close()
        return True
    except:
        return False

def create_shp(tilesize,rndval,outLayer,ul_latitude,ul_longitude,file_pms,src_data,haze,cloud,inc_angle_along,inc_angle_across,date,geometric,spectral,ce90):
	
	lr_latitude=round(ul_latitude-tilesize, rndval)
	lr_longitude=round(ul_longitude+tilesize, rndval)
	
	feature = ogr.Feature(outLayer.GetLayerDefn())
	feature.SetField('Tile_name', os.path.basename(file_pms))
	feature.SetField('Haze', str(haze))
	feature.SetField('Cloud', str(cloud))
	feature.SetField('Src_data', src_data)
	feature.SetField('date', str(date))
	feature.SetField('inc_along', str(inc_angle_along))
	feature.SetField('inc_across', str(inc_angle_across))
	feature.SetField('geometric', geometric)
	feature.SetField('spectral', spectral)
	feature.SetField('ce90', ce90)
	
	# Create test polygon
	ring = ogr.Geometry(ogr.wkbLinearRing)
	ring.AddPoint(ul_longitude, ul_latitude)
	ring.AddPoint(lr_longitude, ul_latitude)
	ring.AddPoint(lr_longitude, lr_latitude)
	ring.AddPoint(ul_longitude, lr_latitude)
	poly = ogr.Geometry(ogr.wkbPolygon)
	poly.AddGeometry(ring)
	feature.SetGeometry(poly)
	return feature;

def main(hostname,db_name,username,db_table,pascode,dest_dir):
    file_shp=dest_dir+os.sep+"mosaik-tile_index20190521.shp"
    # Create the output Driver
    driverName = "ESRI Shapefile"
    outDriver = ogr.GetDriverByName(driverName)
    outDataSource = outDriver.CreateDataSource(file_shp)
    outLayer = outDataSource.CreateLayer(file_shp, geom_type=ogr.wkbPolygon )
    field_name1 = ogr.FieldDefn('Tile_name', ogr.OFTString)
    field_name1.SetWidth(100)
    field_name2 = ogr.FieldDefn('Haze', ogr.OFTString)
    field_name2.SetWidth(100)
    field_name3 = ogr.FieldDefn('Cloud', ogr.OFTString)
    field_name3.SetWidth(10)
    field_name4 = ogr.FieldDefn('Src_data', ogr.OFTString)
    field_name4.SetWidth(10)
    field_name5 = ogr.FieldDefn('date', ogr.OFTString)
    field_name5.SetWidth(10)
    field_name6 = ogr.FieldDefn('inc_along', ogr.OFTString)
    field_name6.SetWidth(10)
    field_name7 = ogr.FieldDefn('inc_across', ogr.OFTString)
    field_name7.SetWidth(10)
    field_name8 = ogr.FieldDefn('geometric', ogr.OFTString)
    field_name8.SetWidth(10)
    field_name9 = ogr.FieldDefn('spectral', ogr.OFTString)
    field_name9.SetWidth(10)
    field_name10 = ogr.FieldDefn('ce90', ogr.OFTString)
    field_name10.SetWidth(10)
    outLayer.CreateField(field_name1)
    outLayer.CreateField(field_name2)
    outLayer.CreateField(field_name3)
    outLayer.CreateField(field_name4)
    outLayer.CreateField(field_name5)
    outLayer.CreateField(field_name6)
    outLayer.CreateField(field_name7)
    outLayer.CreateField(field_name8)
    outLayer.CreateField(field_name9)	
    outLayer.CreateField(field_name10)
    conn = psycopg2.connect(host=hostname, database=db_name, user=username, password=pascode)
    c = conn.cursor()
    c.execute(sql.SQL("SELECT * FROM public.{} WHERE img_date = '2019-05-21';").format(sql.Identifier(db_table)))
    hs = c.fetchall()
    file_pmss=[];
    #ins=[]
    if hs != []:
        for i in hs:
            tilesize = float(i[3])
            rndval=len(str(tilesize).split('.')[-1])
            ul_latitude=float(i[4])
            ul_longitude=float(i[5])
            # lr_latitude=round(ul_latitude-(tilesize+0.00005), 5)
            # lr_longitude=round(ul_longitude+(tilesize+0.00005), 5)
            haze = 'N/A'
            cloud=i[8]
            name=i[0]
            
            if ul_latitude < 0:
                lat="S"+str(ul_latitude*(-1))
            else:
                lat="N"+str(ul_latitude)
            lon='E'+str(i[5])
            if len (lat) == 5:
                new_lat = lat[:1]+'0'+lat[1:]
            elif len(lat) == 4:
                new_lat = lat[:1]+'00'+lat[1:]
            elif len(lat) == 3:
                new_lat = lat[:1]+'000'+lat[1:]
            elif len(lat) == 2:
                new_lat = lat[:1]+'0000'+lat[1:]
            else:
                new_lat = lat
            if len (lon) == 5:
                new_lon = lon[:1]+'0'+lon[1:]
            else:
                new_lon = lon
            source_data=i[9]
            inc_angle_along=i[10]
            inc_angle_across=i[11]
            date=i[12]
            geometric=i[13]
            spectral=i[14]
            ce90=i[15]
            src_data=os.path.basename(source_data)
            if spectral == 'MS':
                dst_file=name[:4]+"_MS_"+new_lat+"_"+new_lon+".tif"
                dst_name=dest_dir+os.sep+dst_file
                file_pms=dst_name.replace("_MS_","_PMS_")
            elif spectral == 'PMS':
                dst_file=name[:4]+"_PMS_"+new_lat+"_"+new_lon+".tif"
                dst_name=dest_dir+os.sep+dst_file
                file_pms=dst_name
            if not file_pms in file_pmss:
                file_pmss.append(file_pms)
                feature=create_shp(tilesize,rndval,outLayer,ul_latitude,ul_longitude,file_pms,src_data,haze,cloud,inc_angle_along,inc_angle_across,date,geometric,spectral,ce90)
                # Create the feature in the layer
                outLayer.CreateFeature(feature)
                feature = None
                # ins.append((source_data,dst_name,ul_longitude,ul_latitude,lr_longitude,lr_latitude,spectral),)
               
    conn.close()            
    outDataSource = None
    

if __name__ == '__main__':
    db_table = 'testSPOT' #sys.argv[4] #'test_db'
    hostname = '192.168.40.34' #sys.argv[5]
    db_name = 'test' #sys.argv[6]
    username = 'administrator' #sys.argv[7]
    pascode = 'teklahta@8' #getpass.getpass()
    dest_dir = r'D:\Training_CSRT\mosaik'
     
    
    if postgres_test(hostname,db_name,username,pascode): 
        main (hostname,db_name,username,db_table,pascode,dest_dir)
    else:
        print('authentication failure. Please check your host, database_name, user and password')
    

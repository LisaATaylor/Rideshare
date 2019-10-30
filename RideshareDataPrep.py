#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 09:31:27 2019

Lisa Taylor

Data transformation/cleaning functions for rideshare dataset
"""

import csv
import os
import matplotlib.pyplot as plt
from datetime import datetime,date,timedelta
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import geopandas as gpd
datadir=r'/Users/rtaylor/Desktop/Springboard/DataSets/Rideshare'

#%%
def load_daily_weather(weathcsv):
    # import raw weather
    weath_df=pd.read_csv(weathcsv)
    weath_df.index=pd.DatetimeIndex(weath_df.DATE)
    
    #extract daily records
    weath_daily=weath_df.loc[weath_df.DailyAverageDryBulbTemperature.notnull(),['DailyAverageDryBulbTemperature','DailyAverageWindSpeed','DailyPrecipitation']]
    weath_daily.columns=['DTemp','DWind','DPrecip']
    weath_daily['DTemp']=pd.to_numeric(weath_daily['DTemp'],errors='coerce').fillna(method='ffill')
    weath_daily['DPrecip']=pd.to_numeric(weath_daily['DPrecip'],errors='coerce').fillna(0)
    weath_daily['Precip']=weath_daily.DPrecip.apply(lambda x: 'Wet' if x>0.01 else 'Dry')
    weath_daily.index=weath_daily.index.normalize()
    return weath_daily
    
def load_hourly_weather(weathcsv):
    # import raw weather
    weath_df=pd.read_csv(weathcsv,low_memory=False)
    weath_df.index=pd.DatetimeIndex(weath_df.DATE)
    #extract hourly records
    weath_hourly=weath_df.loc[weath_df.HourlyDryBulbTemperature.notnull(),['HourlyDryBulbTemperature','HourlyWindSpeed','HourlyPrecipitation']]
    weath_hourly.columns=['HTemp','HWind','HPrecip']
    weath_hourly['HTemp']=pd.to_numeric(weath_hourly['HTemp'],errors='coerce').fillna(method='ffill') #check to see if these should have been interpolated instead
    weath_hourly['HPrecip']=pd.to_numeric(weath_hourly['HPrecip'],errors='coerce').fillna(0)
    weath_hourly['HWind']=pd.to_numeric(weath_hourly['HWind'],errors='coerce').fillna(0)
    weath_hourly['Precip']=weath_hourly.HPrecip.apply(lambda x: 'Wet' if x>0.01 else 'Dry')
    weath_hourly.index=weath_hourly.index.round('H')
    
    #for some reason there are duplicate indices, no time to investigate now so keeping one value
    weath_hourly=weath_hourly.loc[~weath_hourly.index.duplicated(keep='first')]
    return weath_hourly

#weath_csv='/Users/rtaylor/Desktop/Springboard/DataSets/Climate/ChicagoMidway.csv'
#weath_hourly=load_hourly_weather(weath_csv)
    
    
#%%

#function to load census data, data already enriched with spatial, pop density, distance to downtown, income
def load_census():
    censcsv=os.path.join(datadir,'cens_data_wms.csv')
    censdf=pd.read_csv(censcsv)
    censdf['PUCensusTract']=censdf['geoid10'].astype(str)
    #set missing median incomes to average
    avgincome=censdf.loc[censdf.MedIncome>0,'MedIncome'].mean()
    censdf['MedIncome']=censdf['MedIncome'].apply(lambda x: x if x>0 else avgincome)
    censdf.set_index('PUCensusTract',inplace=True)
    #add in distance to downtown
    return censdf

#censfile=os.path.join(datadir,'cens_data_wms.csv')
#cens_df=load_census(censfile)
#%%
#
#%%
def lookup_CommunityAreaSides(CAseries):

    fpath=r'/Users/rtaylor/Desktop/Springboard/DataSets/Rideshare/ChicagoAreas.xlsx'
    sides=pd.read_excel(fpath)
    
    CAdf=CAseries.to_frame(name='ID')
    CAdf=CAdf.merge(sides,left_on='ID',right_on='CA_ID')
    
    return CAdf['Side']

#%%
    #function to compute direction of travel
def get_bearing(pointA,pointB):
    #adapted from on https://gist.github.com/jeromer/2005586
    A_x=pointA.x
    A_y=np.radians(pointA.y)
    B_x=pointB.x
    B_y=np.radians(pointB.y)
    long_diff=np.radians(B_x-A_x)
    x=np.sin(long_diff)*np.cos(B_y)
    y=(np.cos(A_y)*np.sin(B_y)-np.sin(A_y)*np.cos(B_y)*np.cos(long_diff))
    bearing=np.degrees(np.arctan2(x,y))
    if bearing<0:
        bearing=bearing+360
    return bearing

from shapely import wkt
def make_bearing_data():
    #bearing is angle relative to downtown.  N=360, W=270, S=180, E=90
    x=load_raw_rides_data(r'/Users/rtaylor/Desktop/Springboard/DataSets/Rideshare')
    xy=clean_transform_raw_rides_data(x)
    xyg=xy[['PUCensusTract','PU_Geo']].drop_duplicates()
    xyg['geo']=xyg['PU_Geo'].apply(wkt.loads)
    cc=xyg.loc[xyg.PUCensusTract=='17031839100','geo'].iloc[0]
    xyg['bearing']=xyg['geo'].apply(lambda x: get_bearing(x,cc))
    xyg=xyg.set_index('PUCensusTract')
    #gdf=gpd.GeoDataFrame(xyg,geometry='geo')
    #cc=gdf.loc[gdf.PUCensusTract=='17031839100','geo'].iloc[0]#coords of city center
    #gdf['bearing']=gdf['geo'].apply(lambda x: get_bearing(x,cc))
    return xyg



#%%
#get distance to downtown for community areas
def get_CA_dist_to_downtown():
    datadir=r'/Users/rtaylor/Desktop/Springboard/DataSets/Rideshare'
    ca_gdf=gpd.read_file(os.path.join(datadir,'CommAreas.shp'))
    ca_gdf_utm=ca_gdf.to_crs({'init': 'epsg:26916'}) #convert to projected crs for area calc
    #calculate distance from downtown (census tract 17031839100)
    
    ca_gdf_utm['DistToDowntown']=ca_gdf_utm.distance(ca_gdf_utm.loc[ca_gdf_utm.area_num_1=='8'].centroid.iloc[0])/1000
    #return dict of CA, distancc
    return dict(zip(ca_gdf_utm['area_num_1'].astype('int'),ca_gdf_utm['DistToDowntown']))

def get_tract_dist_to_downtown():
    datadir=r'/Users/rtaylor/Desktop/Springboard/DataSets/Rideshare'
    censtracts=gpd.read_file(os.path.join(datadir,'Boundaries - Census Tracts - 2010.geojson'))
    censtracts=censtracts.set_index('geoid10')
    censtracts_utm=censtracts.to_crs({'init': 'epsg:26916'}) #convert to projected crs for area calc
    x=censtracts_utm.loc[censtracts_utm.index=='17031839100'].centroid.x[0]
    y=censtracts_utm.loc[censtracts_utm.index=='17031839100'].centroid.y[0]  
    censtracts_utm['DistToDowntown']=censtracts_utm.distance(censtracts_utm.loc[censtracts_utm.index=='17031839100'].centroid[0])/1000
   #return dict of tract, distancc
    return dict(zip(censtracts_utm.index.values,censtracts_utm['DistToDowntown']))

#tst=get_tract_dist_to_downtown()

#%%
#Get rides data - either sample 12 GB file or load pre-sampled data
#datadir=r'/Users/rtaylor/Desktop/Springboard/DataSets/Rideshare'

def load_raw_rides_data(csvpath,firstrun=False):
    if firstrun:
        iterdf=pd.read_csv(os.path.join(csvpath,'Transportation_Network_Providers_-_Trips.csv'),chunksize=200000)
        #dataset has 40 million records
        samp_df=pd.DataFrame()
        #sample 5% of df and make new df
        for chunk in iterdf:
            samp_df=samp_df.append(chunk.sample(frac=0.05)) #5% of rides
        samp_df=samp_df.dropna(how='any')
        samp_df.columns = ['TripID', 'TripStart', 'TripEnd', 'TripSeconds',
               'TripMiles', 'PUCensusTract', 'DOCensusTract',
               'PUCommunityArea', 'DOCommunityArea', 'Fare', 'Tip',
               'AdditionalCharges', 'TripTotal', 'SharedTripAuth',
               'TripsPooled', 'PU_Lat', 'PU_Long','PU_Geo', 'DO_Lat',
               'DO_Long', 'DO_Geo'] #PU=pick up, DO=drop off
        samp_df['TripStart']=pd.to_datetime(samp_df['TripStart'],infer_datetime_format=True)
        samp_df['TripEnd']=pd.to_datetime(samp_df['TripEnd'],infer_datetime_format=True)
        samp_df.head()
        #this took a long time, saving output
        samp_df.to_pickle(os.path.join(csvpath,'chicago_pickle_05'))
    else:
        samp_df=pd.read_pickle(os.path.join(csvpath,'chicago_pickle_05')) 

    return samp_df

def clean_transform_raw_rides_data(ridesdf):
    ridesdf=ridesdf.copy()
    #Remove rides where fare is zero
    ridesdf=ridesdf.loc[(ridesdf.Fare>0)&(ridesdf.TripMiles>0)]
    #convert census tract fields from float to string
    ridesdf['PUCensusTract']=ridesdf.PUCensusTract.astype(int).astype(str)
    ridesdf['DOCensusTract']=ridesdf.DOCensusTract.astype(int).astype(str)
    #Merge community areas into districts
    ridesdf['PUSide']=lookup_CommunityAreaSides(ridesdf['PUCommunityArea'])
    ridesdf['DOSide']=lookup_CommunityAreaSides(ridesdf['DOCommunityArea'])
    #Derive date/time fields
    ridesdf['TripStartHour']=ridesdf.TripStart.dt.floor('H')
    ridesdf['TripEndHour']=ridesdf.TripEnd.dt.floor('H')
    ridesdf['Year']=ridesdf.TripStart.dt.year
    ridesdf['Month']=ridesdf.TripStart.dt.month
    ridesdf['DOW']=ridesdf.TripStart.dt.dayofweek
    ridesdf['Hour']=ridesdf.TripStart.dt.hour
    #Add bank holidays flag
    bankhols=['01/01/2018','01/15/2018','02/19/2018','05/28/2018','07/04/2018','09/03/2018','10/08/2018','11/12/2018','11/22/2018','12/25/2018','01/01/2019','01/21/2019','02/18/2019','05/27/2019','07/04/2019','09/02/2019','10/14/2019','11/11/2019','11/28/2019','12/25/2019']
    bankhols_dt=[datetime.strptime(x,"%m/%d/%Y") for x in bankhols]
    ridesdf['TripDay']=ridesdf.TripStart.dt.floor('1D')
    ridesdf['IsHoliday']=False
    ridesdf.loc[ridesdf['TripDay'].isin(bankhols_dt),'IsHoliday']=True
    #add flag to indicate period of day
    ridesdf['DayPeriod'] = pd.cut(ridesdf['Hour'], bins=[-1,6, 10, 14, 17, 20, 24], labels=['earlymorning', 'morning', 'midday','afternoon','evening','lateevening'])
    #add ride length categories
    ridesdf['RideLength'] = pd.cut(ridesdf['TripMiles'], bins=[-1,1, 5, 20,100], labels=['0-1', '1-5', '5-20','20+'])
    #weekend vs weekday
    ridesdf['IsWeekend']=ridesdf['DOW'].apply(lambda x: True if x>4 else False)
    #Drop unneeded fields
    ridesdf.drop(['TripID','TripDay'],axis=1,inplace=True)
    #Add distance from Community area to downtown
    dist_dt=get_CA_dist_to_downtown()
    ridesdf['PUCA_DistToDowntown']=ridesdf['PUCommunityArea'].map(dist_dt)
    ridesdf['DOCA_DistToDowntown']=ridesdf['DOCommunityArea'].map(dist_dt)
    
    #Airport flag
    ridesdf['AirportDOorPU']=ridesdf['PUCommunityArea'].isin([64,56,76])|ridesdf['PUCommunityArea'].isin([64,56,76])
    return ridesdf

export=True
if export:
    samp_df=load_raw_rides_data(datadir)
    samp_df_enhanced=clean_transform_raw_rides_data(samp_df)
    samp_df_enhanced.to_pickle(os.path.join(datadir,'cleaned_all'))


#%%
def get_hourly_data(): 
    #generates hourly dtaset with zero ride records for un-utilized tracts
    datadir=r'/Users/rtaylor/Desktop/Springboard/DataSets/Rideshare'
    rawdata=load_raw_rides_data(datadir)
    crdf=clean_transform_raw_rides_data(rawdata) 
    
    #fields2avg=['Fare','Tip','TripTotal','TripMiles','TripSeconds']
    #aggdict=dict(zip(fields2avg,['mean']*len(fields2avg)))
    #aggdict['Hour']=['mean','size']
    #grp_df=crdf.groupby(['PUCensusTract','TripStartHour'])['Fare','Tip','TripTotal','TripMiles','TripSeconds','Hour'].agg(aggdict)
    #grp_df.columns=['MeanFare','MeanTip','MeanTotal','MeanMiles','MeanSeconds']
    
    fields2avg=['Fare','Tip','TripTotal','TripMiles','TripSeconds','Year','Month','DOW','Hour']
    aggdict=dict(zip(fields2avg,['mean']*len(fields2avg)))
    aggdict['Hour']=['mean','size']
    grp_df=crdf.groupby(['PUCensusTract','TripStartHour'])['Fare','Tip','TripTotal','TripMiles','TripSeconds','Year','Month','DOW','Hour'].agg(aggdict)
    grp_df.columns=['MeanFare','MeanTip','MeanTotal','MeanMiles','MeanSeconds','Year','Month','DOW','Hour','NumRides']
    
    #make entries for missing dates and times
    tracts=crdf.PUCensusTract.unique()
    times=crdf.TripStartHour.unique()
    times.sort()
    full_index=pd.MultiIndex.from_product([tracts,times])
    blankdf=pd.DataFrame(index=full_index,columns=grp_df.columns)
    full_df=grp_df.reindex_like(blankdf)
    #fill blanks
    full_df['NumRides'].fillna(0,inplace=True)
    #need to recalculate these for zero ride tracts
    full_df['Year']=full_df.index.get_level_values(1).year
    full_df['Month']=full_df.index.get_level_values(1).month
    full_df['DOW']=full_df.index.get_level_values(1).dayofweek
    full_df['Hour']=full_df.index.get_level_values(1).hour
    full_df['IsWeekend']=full_df['DOW'].apply(lambda x: True if x>4 else False)
    full_df.index.rename(['PUCensusTract','DATE'],inplace=True)
    full_df['date']=full_df.index.get_level_values(1)#temporary for weather merge
    
    bankhols=['01/01/2018','01/15/2018','02/19/2018','05/28/2018','07/04/2018','09/03/2018','10/08/2018','11/12/2018','11/22/2018','12/25/2018','01/01/2019','01/21/2019','02/18/2019','05/27/2019','07/04/2019','09/02/2019','10/14/2019','11/11/2019','11/28/2019','12/25/2019']
    bankhols_dt=[datetime.strptime(x,"%m/%d/%Y") for x in bankhols]
    full_df['TripDay']=full_df.date.dt.floor('1D')
    full_df['IsHoliday']=False
    full_df.loc[full_df['TripDay'].isin(bankhols_dt),'IsHoliday']=True

    #merge weather
    weath_csv='/Users/rtaylor/Desktop/Springboard/DataSets/Climate/ChicagoMidway.csv'
    weath_hourly=load_hourly_weather(weath_csv)
    full_df_weather=full_df.merge(weath_hourly,left_on='date',right_index=True)
    
    #drop unneeded columns
    full_df_weather.drop('date',axis=1,inplace=True)
    full_df_weather.drop('TripDay',axis=1,inplace=True)
    #calculate census tract related fields (prev values based on CA not tract)
    #Airport Flag
    full_df_weather['IsAirport']=0
    full_df_weather.loc[full_df_weather.index.get_level_values(0)=='17031980000','IsAirport']=1
    full_df_weather.loc[full_df_weather.index.get_level_values(0)=='17031980100','IsAirport']=1
    #Load census data
    censdf=load_census()
    full_df_weather_census=full_df_weather.join(censdf)
    full_df_weather_census.drop(['namelsad10','name10','tractce10','statefp10','NAME'],axis=1,inplace=True)
    #load "bearing"
    bear=make_bearing_data()
    full_df_weather_census=full_df_weather_census.join(bear)
    full_df_weather_census.drop(['PU_Geo','geo'],axis=1,inplace=True)
    
    #Get "Side" from community area
    full_df_weather_census['Side']=lookup_CommunityAreaSides(full_df_weather_census['commarea']).values
    #distdict=get_tract_dist_to_downtown()
    #full_df_weather['DistToDowntown']=full_df_weather.index.get_level_values(0).map(distdict)
    return full_df_weather_census
export=False
if export:
    g=get_hourly_data()
    g.to_pickle(os.path.join(datadir,'new_hourly_data_census'))
    
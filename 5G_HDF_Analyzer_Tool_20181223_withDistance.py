# -*- coding: utf-8 -*-
"""
Created on Sun Nov  4 03:46:23 2018

@author: zhendong yang

"""
import pandas as pd
import h5py
import datetime
import os
import time
import math
import csv

def Vlookup_distance(path):
    print("start auto vlookup for distance ")
    handledlist=["test"]
    for file in os.listdir(path):
        filename=os.path.splitext(file)                       
        if filename[0].endswith('distance'):
            with open(path+file) as f:
                distanceinfo = pd.read_csv(f) 
                
    for file1 in os.listdir(path):
        filename=os.path.splitext(file1)                       
        if not filename[0].endswith('distance') and filename[0] not in handledlist:
            handledlist.append(filename[0])
            with open(path+file1) as f:
               dataset = pd.read_csv(f) 
            newdataset=pd.merge(distanceinfo,dataset,on='Time')
            newdataset.to_csv(path+file1,index=False)

    print("auto vlookup for distance is done")

def GPScalculate(GPSfilepath):
    def geodistance(lng1,lat1,lng2,lat2):
        lng1, lat1, lng2, lat2 = map(math.radians, [lng1, lat1, lng2, lat2])
        dlon=lng2-lng1
        dlat=lat2-lat1
        a=math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        dis=2*math.asin(math.sqrt(a))*6371*1000
        return dis
        
    fBSloc=open("BSlocation.txt",'r')
    lbs=fBSloc.readlines()
    blot=lbs[0].replace("\n",'\t')
    blat=lbs[1].replace("\n",'\t')
    blat=float(blat)
    blot=float(blot)
    print("BS location is ",blat,blot)
    
    with open(GPSfilepath+"_GPS_GPS_GPS Out.csv") as f:
        reader = csv.reader(f)
        i=0
        data=[]
        for i in reader:
                if reader.line_num !=1:
                    lat1=""
                    lat2=""
                    lot1=""
                    lot2=""
                    for num in range(3,100):
                        ichr=int(i[num])
                        ichr=chr(ichr)
                        if ichr=="$":
                            for n in range(num+18,num+20):
                                Astr=int(i[n])
                                Astr=chr(Astr)
                                lat1=lat1+Astr
                            lat1=float(lat1)
                            for n in range(num+20,num+27):
                                Astr=int(i[n])
                                Astr=chr(Astr)
                                lat2=lat2+Astr
                            lat2=float(lat2)                            
                            lat=lat1+lat2/60
                            for n in range(num+30,num+33):
                                Astr=int(i[n])
                                Astr=chr(Astr)
                                lot1=lot1+Astr
                            lot1=float(lot1)
                            for n in range(num+33,num+40):
                                Astr=int(i[n])
                                Astr=chr(Astr)
                                lot2=lot2+Astr
                            lot2=float(lot2)
                            lot=lot1+lot2/60
                    distance=geodistance(blot,blat,lot,lat)
                    newGPS=[i[0],lot,lat,distance]
                    data.append(newGPS)
        
        
        with open(GPSfilepath+'_distance.csv', 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            titles=[['Time','longitude','Latitude','distance(m)']]
            csv_writer.writerows(titles)
            csv_writer.writerows(data)
            print("distance calculate is done! ")
            
def SSBRename(pandas_dataframe,i):
    ssbkpilist=["_index","_cellID","_SNR","_RSRP","_SINR","_RSRQ","_freq_error","_time_error_Ts","_pss_exp","_sss_exp","_best_ant_idx"]
    for k in range(12):            
        j=k+1
        j=str(j)
        pandas_dataframe.rename(columns={k:"SSB"+j+ssbkpilist[i]},inplace = True)



def secondlevelKPIOutupt(oneTab,twoTab,kpiname):
    for kpidata in f[oneTab][twoTab][kpiname]:
        timedata=f[oneTab][twoTab][kpiname]["timestamp"]["epoch"]
        timedata=timedata.byteswap().newbyteorder()
        timedata=pd.DataFrame(timedata,columns=["time"])
        #timedata=timedata.drop([len(timedata)-1])
        timedata=timedata.apply(lambda x:datetime.datetime.fromtimestamp(x),axis=1)
        timedata.rename(columns={0:"Time"},inplace = True)
        datablock_All=pd.DataFrame()
        for datalist in f[oneTab][twoTab][kpiname]:
            if datalist !="timestamp":
                datablock=f[oneTab][twoTab][kpiname][datalist].value
                datablock=datablock.byteswap().newbyteorder()
                datablock=pd.DataFrame(datablock)
                datablock.rename(columns={0:"data_0"},inplace = True)
                datablock_All=pd.concat([datablock_All,datablock], axis=1, join='outer') 
        datablock_All=pd.concat([timedata,datablock_All], axis=1,join='outer')
        datablock_All.rename(columns={0:"Time"},inplace = True)
    return datablock_All
    
def SSBKPIOutput(oneTab,twoTab,kpiname):
    for kpidata in f[oneTab][twoTab][kpiname]:
        timedata=f[oneTab][twoTab][kpiname]["timestamp"]["epoch"]
        timedata=timedata.byteswap().newbyteorder()
        timedata=pd.DataFrame(timedata,columns=["time"])
        #timedata=timedata.drop([len(timedata)-1])
        timedata=timedata.apply(lambda x:datetime.datetime.fromtimestamp(x),axis=1)
        timedata.rename(columns={0:"Time"},inplace = True)
        #print(timedata)
        datablock_All=pd.DataFrame()
        for  i in range(11):           
            datablock=f[oneTab][twoTab][kpiname]["data_" + str(i)].value
            datablock=datablock.byteswap().newbyteorder()
            datablock=pd.DataFrame(datablock)
            SSBRename(datablock,i)
            datablock_All=pd.concat([datablock_All,datablock], axis=1, join='outer') 
        #datall.to_csv(oneTab + twoTab + threeTab + kpiname + ".csv",index=False)
        datablock_All=pd.concat([timedata,datablock_All], axis=1,join='outer')
        datablock_All.rename(columns={0:"Time"},inplace = True)
    return datablock_All
    
def creat_folder(dirs,path):    
        #print(file)
        isExists=os.path.exists(dirs+"\\"+path)
        if not isExists:
            print("thatis ok")
            os.makedirs(dirs+"\\"+path) 
        return path
            

if __name__ == "__main__":
    dirs=input("pls input h5file path：")
    GPSswitch=input("if you want calculate the distance, pls input y : ")                       
    for file in os.listdir(dirs):
        filename=os.path.splitext(file)                       
        if filename[1] == ".h5":     
            #print(file)       
            path=creat_folder(dirs,filename[0])
            csvpath=dirs + "\\"+path +"\\"
            h5file=dirs+"\\"+file
            print(csvpath)
            f = h5py.File(h5file,'r')
            for oneTab in f.keys():
                for twoTab in f[oneTab]:
                    for kpiname in f[oneTab][twoTab]:
                        if kpiname =="ssb":
                            print("trying to outpunt KPI："+file+"   "+kpiname)
                            x=SSBKPIOutput(oneTab,twoTab,kpiname)
                            x.to_csv(csvpath+filename[0]+"_"+oneTab +"_"+ twoTab +"_" + kpiname + ".csv",index=False)
                        else:
                            if "fmt" not in kpiname:
                                names=['time','data']
                                print("trying to outpunt KPI："+file+"   "+kpiname)
                                x=secondlevelKPIOutupt(oneTab,twoTab,kpiname)
                                x.to_csv(csvpath+filename[0]+"_"+oneTab +"_"+ twoTab +"_" + kpiname + ".csv",index=False)
            f.close()
#######################计算距离###############################################
            if GPSswitch =="y":
                if os.path.exists(csvpath+filename[0]+"_GPS_GPS_GPS Out.csv"):
                    print("trying to calculate distance......")
                    GPScalculate(csvpath+filename[0])
                    Vlookup_distance(csvpath)
####################################################################                   
    print("all the output is done！，close soon")
    time.sleep(10)
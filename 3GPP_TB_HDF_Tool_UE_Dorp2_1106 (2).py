# -*- coding: utf-8 -*-
"""
Created on Sun Nov  4 03:46:23 2018

@author: elopsuc

"""
import pandas as pd
import h5py
import datetime
import os
import time


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
        #print(timedata)
        datablock=f[oneTab][twoTab][kpiname]["data"].value
        datablock=datablock.byteswap().newbyteorder()
        datablock=pd.DataFrame(datablock)
        datablock.rename(columns={0:"data"},inplace = True)
        datall=pd.concat([timedata, datablock], axis=1, join='outer')
        datall.rename(columns={0:"Time"},inplace = True)
    return datall
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
    
def creat_folder(path):    
        print(file)
        isExists=os.path.exists(path)
        print(isExists)
        if not isExists:
            os.makedirs(path) 
        return path
            

if __name__ == "__main__":

    dirs=input()                       
    for file in os.listdir(dirs):
        filename=os.path.splitext(file)
        if filename[1] == ".h5":     
            print(file)       
            path=creat_folder(filename[0])
            csvpath=os.getcwd() + "\\"+path +"\\"
            f = h5py.File(file,'r')
            for oneTab in f.keys():
                for twoTab in f[oneTab]:
                    for kpiname in f[oneTab][twoTab]:
                        if kpiname !="ssb":
                            names=['time','data']
                            print("以下信息成功输出："+file+"   "+kpiname)
                            x=secondlevelKPIOutupt(oneTab,twoTab,kpiname)
                            x.to_csv(csvpath+filename[0]+"_"+oneTab +"_"+ twoTab +"_" + kpiname + ".csv",index=False)
                        if kpiname =="ssb":
                            print("以下信息成功输出："+file+"   "+kpiname)
                            x=SSBKPIOutput(oneTab,twoTab,kpiname)
                            x.to_csv(csvpath+filename[0]+"_"+oneTab +"_"+ twoTab +"_" + kpiname + ".csv",index=False)
    print("输出已完成！，即将关闭")
    time.sleep(10) 
import pandas as pd
import zipfile
import csv
import os
import re

def combine_csv(zip_name):
    #reading already executed files 
    executed_li = pd.read_csv("executed.csv")["file_name"].to_list()

    #reading zipped folder
    zf = zipfile.ZipFile(os.path.join(os.getcwd(),zip_name),"a")


    if len(executed_li) == 0:
        #creating empty df for the first execution
        combined_df = pd.DataFrame(columns=["Source IP","Environment"])
    else:
        combined_df = pd.read_csv(zf.open('combined.csv'))
        
        '''There is a bug in ZipFile.write() that doesn't overwrites the current file in the zipped folder, 
        instead creates a new file with the same name. Uncomment the following to delete the existing zip.
        There was an issue with the zipped in my system so couldn't test'''
        # from ruamel.std.zipfile import delete_from_zip_file
        # delete_from_zip_file('Engineering Test Files_test.zip', pattern='combined.csv') 
        
        
    for file_name in zf.namelist():
        #checking if file is already executed
        if file_name in executed_li:
            continue
        else:
            executed_li.append(file_name)
        
        #Extracting environment
        try:
            env_name = file_name.replace(".csv",'').split("/")[-1]
        except:
            env_name = file_name.replace(".csv",'')
            
        env_name = re.sub(r'[0-9]+', '', env_name)

        #combining the csv data
        if file_name.endswith(".csv") and "combined.csv" not in file_name:
            file_df = pd.read_csv(zf.open(file_name),usecols = ['Source IP'])  
            file_df['Environment'] = env_name
            combined_df = pd.concat([combined_df,file_df],axis=0,ignore_index=True)

    #writing data into combined.csv
    combined_df.drop_duplicates(subset ="Source IP", inplace = True)
    combined_df.to_csv('combined.csv',index=None,sep=",",header=True,encoding='utf-8')
    zf.write('combined.csv')
    zf.close()

    #to keep it in the zip folder only
    # os.remove('combined.csv')  #uncomment to remove a copy from outside the zip

    #storing the already executed files
    executed_df = pd.DataFrame(executed_li,columns =['file_name'])
    executed_df.to_csv("executed.csv",index=None)


combine_csv('Engineering Test Files.zip')
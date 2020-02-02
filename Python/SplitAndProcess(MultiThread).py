import glob
import os
import pandas as pd
import concurrent.futures
import shutil
from datetime import datetime
from calendar import monthrange

#===================================================================================
def processData(FileList):
#===================================================================================

    #this function is to read and save the processed csv with the correct encoding into the relevant folders
    startTime_sub = datetime.now()
    Name = os.path.basename(FileList)
    print('LOAD : ' + Name + '  -  ' + str(startTime_sub))

    #check file for header row
    with open(FileList) as myfile:
        head = [next(myfile) for x in range(1)]
        #convert list to string
        head = ' '.join([str(elem) for elem in head])
        print(head)

    #if there is a header , set header row else  set header as NONE
    #finds the specified name in variable checkHeader
    if checkHeader.upper() in head.upper():
        headOffset = 0
    else:
        headOffset = None

    #Read file with multiple Encoding types
    try:
        df_Data = pd.read_csv(FileList, sep="|", dtype=str, header= headOffset)
    except:
        try:
            print('UTF-8 Failed')
            df_Data = pd.read_csv(FileList, sep="|", dtype=str, encoding='cp949',header= headOffset)
        except:
            print('cp949 Failed')
            df_Data = pd.read_csv(FileList, sep="|", dtype=str, encoding='cp1251', header=headOffset)

    #addHeaders
    if headOffset is None:
        df_Data.columns = headerNames

    print (df_Data.head(5))
    print('----SAVE : ' + Name + '  -  ' + str(datetime.now() - startTime_sub) + "\t RowsCount: " + str(len(df_Data)))

    #Get Month Date from Dataframe
    dataDate = df_Data.loc[1,'CLOG_YYMM']
    dataDate = str(dataDate)
    year = int(dataDate[:4])
    mth = int(dataDate[5:6])

    #Find Last day
    lastDay = monthrange(year,mth)[1]
    lastDay =  "{0:0=2d}".format(lastDay)
    mth = "{0:0=2d}".format(mth)
    fullDate = str(year) + str(mth) + str(lastDay)


    #to set the number if file exist incrementally
    i = 1
    ext = '.txt'
    fileName = '<<FILENAME>>' + str(i) + '_' + fullDate + '_' + dataDate + ext

    print('Name: ' + fileName)

    #check if the file number exist, if exist add a number behind
    while os.path.exists(procPath + fileName):
        i = i + 1
        fileName = 'KR_KR_MERITZ_SIPROD' + str(i) + '_' + fullDate + '_' + dataDate + ext
        #print(name + str(i) )

    #fileName = 'KR_KR_MERITZ_SIPROD' + str(i) + '_' + fullDate + '_' + dataDate + ext
    print (fileName)

    df_Data.to_csv(procPath + fileName, sep='|', index=False, encoding='UTF-8')
    print('####COMP : ' + Name + '  -  ' + str(datetime.now() - startTime_sub))
    del df_Data
    shutil.move(FileList, archivePath+Name)



###################################
##### PYTHON CONSOLE SETTINGS #####
###################################
startTime = datetime.now()
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 150)
pd.set_option('display.width', 200)
pd.options.display.float_format = '{:,.2f}'.format

#########################
##### FILE SETTINGS #####
#########################
#PROGRAM WILL READ FROM THE FILEPATH FOLDER, IT WILL PROCESS AND SAVE INTO THE SAVEPATH, THE ORIGINAL FILE WILL BE MOVED TO ARCHIVE PATH
fpath = r'<<FILEPATH>>'
fname = '*.txt'
procPath = r'<<SAVEPATH>>'
archivePath =r'<<ARCHIVEPATH>>'
checkHeader = 'CLOG_YYMM'
headerNames = ['CLOG_YYMM', 'POL_NO', 'SBCP_DT', ...]

#########################
##### PROCESS FILES #####
#########################
#50 THREADS ARE OPEN TO RUN AS MANY AS 50 FILES AT THE SAME TIME
with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
    for FileList in glob.glob(fpath + fname):
        executor.submit(processData, FileList)


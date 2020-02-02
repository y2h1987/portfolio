''' This program splits the data into multiple chunks and then using multiple threads to parse data into
    an SQL Table '''

import pandas as pd
import concurrent.futures
import sqlalchemy as sa
import math
import sys
import glob
import os
import shutil
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Float, Integer, String, MetaData, ForeignKey, Unicode
from sqlalchemy.pool import QueuePool
from datetime import datetime


#Python Console Settings
startTime = datetime.now()
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 150)
pd.set_option('display.width', 200)
pd.options.display.float_format = '{:,.2f}'.format

def RunD(df_chunk, i):
    ''' This function converts the dataframe into a dictionary for bulk insert into sql'''

    # start conversion
    print("[" + str(i) + "] - Converting Data to Dict")
    startTime_sub_01 = datetime.now()
    listToWrite = df_chunk.to_dict(orient='records')
    print("[" + str(i) + "] - Completed Data to Dict : " + str(datetime.now() - startTime_sub_01))

    #load to SQL
    print("[" + str(i) + "] - Metadata")
    metadata = sa.schema.MetaData(bind=engine)
    metadata.reflect(engine, only=[tableName])
    Session = sessionmaker(bind=engine)
    session = Session()
    print("[" + str(i) + "] - Loading to SQL")
    startTime_sub_02 = datetime.now()
    session.bulk_insert_mappings(schema_Meritz, listToWrite)
    session.commit()
    session.close()
    print("[" + str(i) + "] - Completed : " + str(datetime.now() - startTime_sub_02))


#SQL Settings
databaseName = 'SQL_DB'
tableName = 'SQL_TABLE'
connectionString = 'mssql+pyodbc://<<ID>>/' + databaseName + '?Driver={SQL Server Native Client 11.0}'

#Create SQL Connection
engine = sa.create_engine(connectionString, poolclass=QueuePool, pool_size=10, fast_executemany=True)
Base = declarative_base()
conn = engine.connect()

#Create Schema for Table
class schema_Meritz(Base):
    __tablename__ = tableName
    COL1 = Column(Integer,primary_key= True)
    COL2 = Column(Integer)
    COL3 = Column(String(20))
    COL4 = Column(String(10))
    COL5 = Column(Unicode(50,collation='Korean_Wansung_CI_AS'))
    COL6 = Column(Unicode(50,collation='Korean_Wansung_CI_AS'))  #collation is done do enable column to accept korean language
    COL7 = Column(Float)
    COL8 = Column(String(10))
    COL9 = Column(String(10))
    COL10 = Column(Float)
    COL11 = Column(Float)
    COL12 = Column(String(20))
    COL13 = Column(String(20))
    COL14 = Column(String(10))
    COL15 = Column(String(10))
    COL16 = Column(String(10))
    COL17 = Column(String(255))
    #returns object representation
    def __repr__(self):
        return ""

#Create table if it doesnt exists
if not engine.dialect.has_table(engine,tableName):
    print("No Existing Table Found")
    schema_Meritz.__table__.create(bind=engine, checkfirst=True)
    print("Created Table " + tableName)

#Set Raw Data Path and Name
fpath = r'<<FILEPATH>>'
fpath_name = '*.txt'
archivePath =r'<<ARCHIVEPATH>>'

#Execute the load for each file in folder
for FileList in glob.glob(fpath + fpath_name):
    fname = os.path.basename(FileList)
    print(fname)
    #Check if Table has been loaded before
    SQLQuery = "SELECT TOP(5) [FNAME] FROM [%(database)s].[dbo].[%(table)s] where [FNAME] = ?" % {'database' : databaseName,'table':tableName}
    df_exist = pd.read_sql_query(SQLQuery, params=[fname], con=conn)
    print(df_exist)

    #if data has not been loaded before
    if len(df_exist) == 0 :
        #Read and process raw data
        df = pd.read_csv(fpath + fname, sep='|', dtype=str, header=0)
        df.reset_index(inplace = True)
        df['FNAME'] = fname
        print(df.head(5))

        #Break raw data into multiple sections for loading
        sections = 5
        rowsperSection = len(df)/sections
        rowsperSection = math.ceil(rowsperSection)
        dict_sections = {}
        startRow = 0
        endRow = rowsperSection
        checkRow = 0

        for i in range(sections):
            dict_sections[i] = df[startRow:endRow]
            startRow = endRow
            endRow = endRow + rowsperSection
            checkRow = checkRow + len(dict_sections[i])

        if checkRow == len(df):
            print("Rows Verified (checkrow vs original): " + str(checkRow) + " vs " + str(len(df)))
        else:
            print("Rows mismatch (checkrow vs original): " + str(checkRow) + " vs " + str(len(df)))
            sys.exit()

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            for i in range(sections):
                executor.submit(RunD, dict_sections[i], i)

        print('RUN COMPLETED : ' + str(datetime.now() - startTime))

    else:
        print("Data Exists in table")

    shutil.move(FileList, archivePath + fname)

###This function sorts the data into their respective section based on a decision tree matrix. 

import pandas as pd
import numpy as np
from datetime import datetime
global startTime
import sys

sys.path.insert(0, '')
from funct_General import progTimer

pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 150)
pd.set_option('display.width', 200)
pd.options.display.float_format = '{:,.2f}'.format
MainStartTime = datetime.now()

def sortData(df_Data):
    ############################
    ##### GET MAPPING DATA #####
    ############################
    progTimer('RETRIEVE MAPPINGS DATA',True)

    #Mapping Details
    path_dataMaps =
    name_refData = 
    name_LimitOk = 

    #OK_LIMIT
    df_limitOK = pd.read_csv(path_dataMaps + name_LimitOk,header=None, dtype=str )
    df_limitOK.rename(columns={2: "CLOG_YYMM", 3: "POL_NO"}, inplace=True)
    df_limitOK = df_limitOK[['POL_NO']]
    df_limitOK.drop_duplicates(inplace=True)
    list_limitOK = list(df_limitOK['POL_NO'])

    #REFERENCE DATA - Product
    shtRef_product = 'Product_Codes'
    df_refProduct = pd.read_excel(path_dataMaps + name_refData, sheet_name=shtRef_product, header=0 , dtype=str )
    df_refProduct = df_refProduct[['PROD_CODE','SAP_TREATY','SECTION_NO','PRODUCT_SUFFIX_02','PRODUCT_SUFFIX_04','PRODUCT_SUFFIX_05']]
    df_refProduct.rename(columns={"PROD_CODE": "UNT_PD_CD"}, inplace=True)
    df_refProduct_25 = df_refProduct[df_refProduct['SAP_TREATY'] == '100XXXXXXX']  #Split Ref into Ref for Treaty '25' and '81'
    df_refProduct_81 = df_refProduct[df_refProduct['SAP_TREATY'] == '100XXXXXX1']
    df_refProduct = df_refProduct[df_refProduct['PRODUCT_SUFFIX_05'] != 'X'] #Remove lines that has an X in Product Suffix 05

    #REFERENCE DATA - Benefits
    shtRef_Bene = 'Benefit_Codes'
    df_refBene = pd.read_excel(path_dataMaps + name_refData, sheet_name=shtRef_Bene, header=0 , dtype=str )
    df_refBene = df_refBene[['BENE_CODE','ACCOUNTING_UNITS','PLAN_CODE','PCT_SCOPE','WAIT_PRD_PCT','MULTIPLIER_VAL','REDUCTION_PCT','BENE_SUFFIX01']]
    df_refBene.rename(columns={"BENE_CODE": "COV_CD"}, inplace=True)
    df_refBene[['REDUCTION_PCT','PCT_SCOPE','MULTIPLIER_VAL','WAIT_PRD_PCT']] = df_refBene[['REDUCTION_PCT','PCT_SCOPE','MULTIPLIER_VAL','WAIT_PRD_PCT']].fillna(1)

    #REFERENCE DATA - NONCANCER LIST
    df_nonCancer = df_refBene[['COV_CD','BENE_SUFFIX01']][df_refBene['BENE_SUFFIX01']=='3Q']
    list_nonCancer = list(df_nonCancer['COV_CD'])

    progTimer('RETRIEVE MAPPINGS DATA', False)

    ######################
    ##### MERGE DATA #####
    ######################
    progTimer('MERGE MAPPINGS DATA', True)
    df_subData = df_Data
    df_subData = df_subData.drop(columns='UNT_PD_NM')  # remove korean plan description column
    df_subData = pd.merge(df_subData, df_refBene, how='left', left_on='COV_CD',right_on='COV_CD')  # merge the reference product info
    df_subData = pd.merge(df_subData, df_refProduct, how='left', left_on='UNT_PD_CD',right_on='UNT_PD_CD')  # merge the summary info
    df_subData = df_subData.fillna('NA')  # remove Nulls
    df_subData['QNUM'] = np.nan
    df_subData['SUM_REINSURED'] = np.nan
    df_subData['RI_RATE'] = np.nan
    progTimer('MERGE MAPPINGS DATA', False)

    ###########################
    ##### CAST DATA TYPES #####
    ###########################
    progTimer('CAST DATA TYPE', True)
    columnToFloat = ['SBC_AMT','CSS_PREM','REDUCTION_PCT','PCT_SCOPE','MULTIPLIER_VAL','WAIT_PRD_PCT']
    columnToInt = ['SECTION_NO']
    df_subData[columnToFloat] = df_subData[columnToFloat].astype(float)
    df_subData[columnToInt] = df_subData[columnToInt].astype(int)
    df_subData['DATE_01'] = df_subData['CLOG_YYMM'].astype(str) + '01'  # covert date field from CLOG_YYMM
    df_subData['DATE_01'] = pd.to_datetime(df_subData['DATE_01'], infer_datetime_format=True)
    df_subData['SBCP_DT'] = pd.to_datetime(df_subData['SBCP_DT'], infer_datetime_format=True)
    progTimer('CAST DATA TYPE', False)

    #################################
    ##### DATETIME CALCULATIONS #####
    #################################
    progTimer('DATE TIME CALCULATION', True)
    from pandas.tseries.offsets import MonthEnd, MonthBegin
    df_subData['MTH_BEG'] = pd.to_datetime(df_subData['SBCP_DT']) + MonthBegin()
    df_subData['NB_MTHS'] = df_subData['DATE_01'] - df_subData['MTH_BEG']
    df_subData['NB_MTHS'] = df_subData['NB_MTHS']/np.timedelta64(1,'M')
    df_subData['NB_MTHS'] = df_subData['NB_MTHS'].astype(int)
    df_subData['NB_MTHS'] = df_subData['NB_MTHS'] + 1
    df_subData['COND_TYPE'] = '1100'
    df_subData.loc[df_subData['NB_MTHS'] >= 12, 'COND_TYPE'] = '1200'
    progTimer('DATE TIME CALCULATION', False)

    #################################################
    ##### DECISION TREE for splitting 1Q and 3Q #####
    #################################################

    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    #>>>>> SPLIT POLICIES --- LIMIT OK >>>>>
    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    progTimer('SPLIT OK LIMIT', True)
    #GET OK LIMIT POLICIES
    df_subData_limitOK = df_subData.loc[df_subData['POL_NO'].isin(list_limitOK)].copy()

    #IF THERE ARE OK LIMIT POLICIES , ASSIGN VALUE TO QNUM AND SECTION NUMBER
    if len(df_subData_limitOK) > 0:
        df_subData_limitOK.loc[:, 'QNUM'] = 'LIMT'
        df_subData_limitOK.loc[:, 'SECTION_NO'] = 14

    #REMOVE OK LIMIT POLICIES FROM THE MAIN DATA POOL
    df_subData = df_subData.loc[~df_subData['POL_NO'].isin(list_limitOK)]
    progTimer('SPLIT OK LIMIT', False)

    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    #>>>>> DECISION_TREE 01 --- NOT A SI CANCER PRODUCT (LIST 1Q) >>>>>
    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    progTimer('DECISION TREE 01', True)
    #PRODUCT CODES FOR SI CANCER THAT ARE STILL BUNDLED WITH OLD TREATY
    list_1Q = ['61137', '61154', '61201', '61203']

    #GET POLICIES THAT BELONGS TO 100025 AND THOSE NOT IN LIST 1Q
    df_subData_old3Q = df_subData.loc[(~df_subData['UNT_PD_CD'].isin(list_1Q)) & (df_subData['SAP_TREATY']=='100XXXXXXX')].copy()
    #IF THERE df_subData_old3Q IS NOT EMPTY , ASSIGN VALUE TO QNUM
    if len(df_subData_old3Q) > 0:
        df_subData_old3Q.loc[:, 'QNUM'] = '3Q_ORI'
    #KEEP THOSE THAT ARE IN LIST 1Q AND KEEP THOSE IN TREATY 1000081
    df_subData = df_subData.loc[(df_subData['UNT_PD_CD'].isin(list_1Q)) | (df_subData['SAP_TREATY']=='100XXXXXX1')]
    progTimer('DECISION TREE 01', False)

    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    #>>>>> DECISION_TREE 02 --- IS a SI CANCER Product (List_1Q) AND policies that DO NOT HAVE NON_CANCER RIDERS >>>>>
    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    progTimer('DECISION TREE 02', True)
    #GET POLICIES THAT HAS NON-CANCER RIDERS
    '''This Segment finds filter for coverage code that are in the list of non cancer riders
       The policies is retrieved from the filtered segment 
       Duplicates are dropped to form the list of policie that HAVE non_cancer riders'''
    df_subData_NC = df_subData[df_subData['COV_CD'].isin(list_nonCancer)]
    df_subData_NC = df_subData_NC[['POL_NO']]
    df_subData_NC = df_subData_NC.drop_duplicates()
    list_polwNonCancer = list(df_subData_NC['POL_NO'])

    #GET PRODUCT THAT DOES NOT CONTAIN NON-CANCER BENEFITS
    df_subData_1Q = df_subData.loc[~df_subData['POL_NO'].isin(list_polwNonCancer)].copy()
    #IF THERE df_subData_1Q IS NOT EMPTY , ASSIGN VALUE TO QNUM
    if len(df_subData_1Q) > 0:
        df_subData_1Q.loc[:, 'QNUM'] = '1Q'
    #REMOVE POLICIES THAT DOES NOT CONTAIN NON-CANCER FROM THE DATAPOOL
    df_subData = df_subData.loc[df_subData['POL_NO'].isin(list_polwNonCancer)]
    progTimer('DECISION TREE 02', False)

    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    #>>>>> DECISION_TREE 03 --- IS a SI CANCER Product (List_1Q) AND policies that HAS NON_CANCER RIDERS >>>>>
    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    progTimer('DECISION TREE 03', True)
    #GET POLICIES THAT ARE 1000081 , AND POLICIES WITH TAG 1811 INCEPTED AFTER 2018-12-31
    df_subData_new3Q = df_subData.loc[((df_subData['SBCP_DT'] > '2018-12-31') &
                                       (df_subData['PRODUCT_SUFFIX_02'] == '1811')) |
                                       (df_subData['SAP_TREATY']=='100XXXXXX1')].copy()
    #IF THERE df_subData_new3Q IS NOT EMPTY , ASSIGN VALUE TO QNUM
    if len(df_subData_new3Q) > 0:
        df_subData_new3Q.loc[:, 'QNUM'] = '3Q_N'
    #REMAINDER GOES INTO OLD TREATY
    df_subData_old3Qpart2 = df_subData.loc[~df_subData['POL_NO'].isin(list(df_subData_new3Q['POL_NO']))].copy()
    #IF THERE df_subData_old3Qpart2 IS NOT EMPTY , ASSIGN VALUE TO QNUM
    if len(df_subData_old3Qpart2) > 0:
        df_subData_old3Qpart2.loc[:, 'QNUM'] = '3Q'

    del df_subData
    progTimer('DECISION TREE 03', False)

    ################################################
    ##### COMBINE SPILT TO RESPECTIVE TREATIES #####
    ################################################
    progTimer('COMB SPLIT', True)
    df_subData_OldTreaty = pd.concat([df_subData_old3Q, df_subData_old3Qpart2],sort=True)
    df_subData_OldTreaty.loc[((df_subData_OldTreaty['SBCP_DT'] > '2018-12-31') &
                              (df_subData_OldTreaty['PRODUCT_SUFFIX_02'] == '1810')), 'SECTION_NO'] = '12'

    df_subData_NewTreaty = pd.concat([df_subData_1Q, df_subData_new3Q],sort=True)

    # REASSIGN TREATY NUMBER AND SECTION ID --- NEW TREATY DATA
    df_subData_NewTreaty['SAP_TREATY'] = '100XXXXXX1'
    df_subData_NewTreaty.drop(['SECTION_NO'], axis=1, inplace=True)
    df_refProduct_81.rename(columns={"PRODUCT_SUFFIX_04": "QNUM"}, inplace=True)
    df_subData_NewTreaty = pd.merge(df_subData_NewTreaty,
                                    df_refProduct_81[['UNT_PD_CD', 'SECTION_NO', 'QNUM']], how='left',
                                    left_on=['UNT_PD_CD', 'QNUM'], right_on=['UNT_PD_CD', 'QNUM'])

    progTimer('COMB SPLIT', False)
    df_Comb = pd.concat([df_subData_limitOK, df_subData_OldTreaty,df_subData_NewTreaty],sort=True)
    return df_Comb

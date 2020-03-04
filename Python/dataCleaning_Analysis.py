# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 14:29:33 2019


@author: Ryeo
"""

import glob
import os
import pandas as pd
import numpy as np

pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 150)


def summarize_Data(df_all, hd_closeMth, retainZero=True, savehdf = False, hdf_path = ''):
    # CREATE A TABLE WITH THE DATA COLUMN NAMES ON ROWS
    df_analysis = pd.DataFrame({'Header_Name': list(df_all.columns),
                                'Header_Type': list(df_all.dtypes)})

    # CREATE LISTS TO HOLD ANALYSIS
    AvgLen, MinLen, MaxLen, Nulls, Compare, UniqueVal = ([] for i in range(6))

    # ANALYSIS OF AVERAGE LENGTH OF VALUE IN EACH COLUMN IN THE MAIN DATA
    for index, row in df_analysis.iterrows():

        HeaderName = str(row['Header_Name'])
        print(HeaderName)
        AvgLen.append(df_all[HeaderName].astype(str).apply(len).mean())
        MinLen.append(df_all[HeaderName].astype(str).apply(len).min())
        MaxLen.append(df_all[HeaderName].astype(str).apply(len).max())
        Nulls.append(df_all[HeaderName].isnull().sum(axis=0))
        UniqueVal.append(df_all[HeaderName].nunique())

    df_analysis['AvgLen'] = AvgLen
    df_analysis['MinLen'] = MinLen
    df_analysis['MaxLen'] = MaxLen
    df_analysis['StdLen'] = df_analysis.apply(lambda row: True if (round(row.AvgLen) - row.AvgLen) == 0 else False,
                                              axis=1)
    df_analysis['Nulls'] = Nulls
    df_analysis['UnqCnt'] = UniqueVal
    # COMPARATIVE ANALYSIS , COMPARE MONTH ON MONTH DATA TO GET DIFFERENCE / MOVEMENT
    # GET UNIQUE LIST OF MONTHS
    mthsInFile = df_all[hd_closeMth].unique().tolist()

    # CREATE DICTIONARY TO HOUSE MONTHS
    d_mths = {}
    for i in mthsInFile:
        d_mths[i] = df_all.loc[df_all[hd_closeMth] == i]

    # CREATE DICTIONARY TO HOUSE DATA
    CompMonth, CompResult = ({} for i in range(2))
    for index, i in enumerate(mthsInFile):
        # if it is not the last item in mthsInfile
        if index != max(enumerate(mthsInFile))[0]:
            # loop through the analysis table for the header names
            for index2, row in df_analysis.iterrows():

                HeaderName = str(row['Header_Name'])
                print('Comparing: ' + mthsInFile[index] + "/" + mthsInFile[index + 1] + ' ' + HeaderName)
                if HeaderName != 'UID':
                    df_temp01 = d_mths[mthsInFile[index]][['UID', HeaderName]]
                    df_temp02 = d_mths[mthsInFile[index + 1]][['UID', HeaderName]]

                    # Compare Columns
                    df_checks = pd.merge(df_temp01[['UID', HeaderName]],
                                         df_temp02[['UID', HeaderName]], how='left', left_on='UID', right_on='UID')
                    df_checks = df_checks.dropna()
                    df_checks['Compare'] = df_checks[HeaderName + '_x'] == df_checks[HeaderName + '_y']
                    result = df_checks['Compare'][df_checks['Compare'] == False].count()

                    CompResult[HeaderName] = df_checks[df_checks['Compare'] == False]

                    if not retainZero:
                        if (result == 0):
                            CompResult[HeaderName] = None
                        else:
                            CompResult[HeaderName] = df_checks[df_checks['Compare'] == False]

                    Compare.append(result)
                    del df_checks, df_temp01, df_temp02
                else:
                    result = 0
                    Compare.append(result)

                print('{:<18s}{:<15s}{:<12s}{:<30s}'.format('Processed: ',
                                                            mthsInFile[index] + "/" + mthsInFile[index + 1],
                                                            str(result), HeaderName))
                del result


            if savehdf:
                CompResult.to_hdf(hdf_path, key=mthsInFile[index] + "/" + mthsInFile[index + 1])

            CompMonth[mthsInFile[index] + "/" + mthsInFile[index + 1]] = CompResult
            print("-" * 80)
            df_analysis[mthsInFile[index] + "/" + mthsInFile[index + 1]] = Compare
            del Compare, CompResult
            CompResult = {}
            Compare = []

    return df_analysis, CompMonth

''' Prompt:
A member has executed several contact programs in the state of Texas and has asked us to help them analyze the data across them all. They have sent us two csv files, one for each county that they worked in. 
Before we can begin analyzing the data, we need to merge it and import it into our redshift database. 
We notice upon initial inspection of the data that it is messy and not formatted correctly for importing.

In order to upload this file into our database, the following conditions must be met:
There can be no duplicate column names
There can be no white space in the column headers
Column names cannot include any of the following characters: [ ? ,  ! . & $ %  ] 
Column names should be all lowercase

Write a script that automatically converts the two csv files into a format that can be uploaded into our database, and merges the two files together into one dataset with one row per respondent. 
We would like to automate the upload process, so rather than making your process specific to each file, write a script that could be used on future datasets that have similar formatting issues. 
You can assume that the “unique_id” column appears only once and is not repeated between files. 
'''

import pandas as pd
import glob

#funtion to read csv into dataframe and transform column names (including adding '.n' to duplicate column names)
def transformfile(file):
    df = pd.read_csv(file)
    df.columns = df.columns.str.replace(r'\s+|[ ? ,  ! . & $ % _]+','').str.lower()
    df.columns = pd.io.parsers.ParserBase({'names':df.columns})._maybe_dedup_names(df.columns)
    return df 

#read transform all files, remove duplicate rows, remove '.' from any duplicate column names
df_all = pd.concat([transformfile(f) for f in glob.glob('~/files2transform/*.csv')])
df_all.drop_duplicates(keep='first', inplace=True)
df_all.columns = df_all.columns.str.replace('.','') 


#write transformed data to new csv
df_all.to_csv(path_or_buf='~/transformed_files/all_wrangled.csv')

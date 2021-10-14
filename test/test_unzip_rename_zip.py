# -*- coding: utf-8 -*-
"""
Created on Wed Jul 14 10:04:48 2021

@author: User
"""

import os
import sys
import zipfile
import chardet
import pandas as pd
import sqlalchemy as db
import hashlib
import base64
import re
import traceback
from datetime import datetime

import csv
import shutil
import fileinput


PATH = r'C:\Users\User\IdeaProjects\aktin_python_scripts\p21.zip'
CSV_SEPARATOR = ';'
CSV_BYTES_CHECK_ENCODER=1024

DICT_P21_COLUMNS = {
        'fall.csv': ['kh-internes-kennzeichen', 'ik-der-krankenkasse', 'geburtsjahr', 'geschlecht', 'plz', 'aufnahmedatum',
                     'aufnahmegrund', 'aufnahmeanlass', 'fallzusammenführung', 'fallzusammenführungsgrund', 'verweildauer-intensiv',
                     'entlassungsdatum', 'entlassungsgrund', 'beatmungsstunden', 'behandlungsbeginn-vorstationär',
                     'behandlungstage-vorstationär', 'behandlungsende-nachstationär', 'behandlungstage-nachstationär'],
        'fab.csv': ['kh-internes-kennzeichen', 'fachabteilung', 'fab-aufnahmedatum', 'fab-entlassungsdatum', 'kennung-intensivbett'],
        'icd.csv': ['kh-internes-kennzeichen', 'diagnoseart', 'icd-version', 'icd-kode', 'lokalisation', 'diagnosensicherheit',
                    'sekundär-kode', 'sekundär-lokalisation', 'sekundär-diagnosensicherheit'],
        'ops.csv': ['kh-internes-kennzeichen', 'ops-version', 'ops-kode', 'ops-datum', 'lokalisation']
}


def get_csv_encoding(path_csv):
   return chardet.detect(open(path_csv, 'rb').read(CSV_BYTES_CHECK_ENCODER))['encoding']

def toLowerCase(string):
    return string.lower()


# create new folder for tmp
##############################
ind = PATH.rindex(os.path.sep)
PATH_TMP_FOLDER = os.path.join(PATH[:ind+1], 'tmp')

if not os.path.isdir(PATH_TMP_FOLDER):
    os.makedirs(PATH_TMP_FOLDER)
##############################


# extract zip to tmp
##############################
with zipfile.ZipFile(PATH, 'r') as file_zip:
    file_zip.extractall(PATH_TMP_FOLDER)
##############################


#check_csv_names
##############################
set_required_csv = set(DICT_P21_COLUMNS.keys())
list_files_dir = [file for file in os.listdir(PATH_TMP_FOLDER) if os.path.isfile(os.path.join(PATH_TMP_FOLDER, file))]
for file in list_files_dir:
    os.rename(os.path.sep.join([PATH_TMP_FOLDER, file]), os.path.sep.join([PATH_TMP_FOLDER, file.lower()]))
set_files_lower = set(map(lambda x: x.lower(), list_files_dir))
set_matched_csv = set_required_csv.intersection(set_files_lower)
if 'fall.csv' not in set_matched_csv:
    raise SystemExit('fall.csv is missing in zip')
if set_matched_csv != set_required_csv:
    print('following csv could not be found in zip: {0}'.format(set_required_csv.difference(set_matched_csv)))
#return set_matched_csv
##############################






# make all columns in csv lowercase
##############################
for name_csv in set_matched_csv:
    path_csv = os.path.sep.join([PATH_TMP_FOLDER, name_csv])

    encoding=get_csv_encoding(path_csv);
    df = pd.read_csv(path_csv, sep=CSV_SEPARATOR, encoding=encoding)


    df = pd.read_csv(path_csv, nrows=1, index_col=None, sep=CSV_SEPARATOR, encoding=encoding)
    df.rename(columns=str.lower, inplace=True)

    if(name_csv == 'icd.csv'):
        index = df.columns.get_loc('sekundär-kode')
        if('lokalisation.1' in df.columns[index:]):
            df.rename(columns={'lokalisation.1':'sekundär-lokalisation'}, inplace=True)
        if('diagnosensicherheit.1' in df.columns[index:]):
            df.rename(columns={'diagnosensicherheit.1':'sekundär-diagnosensicherheit'}, inplace=True)


    header_row = ';'.join(df.columns) + '\n'

    with open(path_csv, "r+") as f1, open(path_csv, "r+") as f2:
        f1.readline() # to move the pointer after header row
        f2.write(header_row)
        shutil.copyfileobj(f1, f2) # copies the data rows

    df = pd.read_csv(path_csv, sep=CSV_SEPARATOR, encoding=encoding)
    #print(df)
   # print(list(df.columns.values))
   # print(df.columns)

    #df = df.rename(columns=toLowerCase).head()
    #df.to_csv(path_csv,sep=';', encoding=encoding)
    #list(data.columns.values.tolist())

##############################

#shutil.rmtree(PATH_TMP_FOLDER)

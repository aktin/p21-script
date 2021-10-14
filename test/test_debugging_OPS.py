# -*- coding: utf-8 -*-
"""
Created on Mon Jun 28 16:57:26 2021

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


def getIdOfHash(num):
    path_csv = r"C:\Users\User\Desktop\ops\OPS.csv";
    encoding = "";
    with open(path_csv, 'rb') as rawdata:
        encoding = chardet.detect(rawdata.read(1024))['encoding']

    for chunk in pd.read_csv(path_csv, chunksize=1000, sep=";", encoding=encoding, dtype=str):
        chunk = chunk['KH-internes-Kennzeichen']
        for extension in chunk:
            composite = '/'.join(["1.2.276.0.76.3.87686.1.45", extension])
            buffer = composite.encode('UTF-8')
            alg = hashlib.sha1()
            alg.update(buffer)
            hashed_id = base64.urlsafe_b64encode(alg.digest()).decode('UTF-8')
            if hashed_id == num:
                print(extension)
                break

def getRowsOfNum(num):
    path_csv = r"C:\Users\User\Desktop\ops\OPS.csv";
    encoding = "";
    dataframe = pd.DataFrame();
    with open(path_csv, 'rb') as rawdata:
        encoding = chardet.detect(rawdata.read(1024))['encoding']
    for chunk in pd.read_csv(path_csv, chunksize=1000, sep=";", encoding=encoding, dtype=str):
        dataframe = dataframe.append(chunk[chunk['KH-internes-Kennzeichen'] == num])
    print(dataframe[['OPS-Kode','OPS-Version','Lokalisation','OPS-Datum']])


def convert_ops_code_to_i2b2_format(code_ops):
    code_ops = ''.join([code_ops[:1], '-', code_ops[1:]] if code_ops[1] != '-' else code_ops)
    if len(code_ops) > 5:
        code_ops = ''.join([code_ops[:5], '.', code_ops[5:]] if code_ops[5] != '.' else code_ops)
    return code_ops

def getAllValidCases():
    path_csv = r"C:\Users\User\Desktop\ops\all_valid_patients.csv";
    encoding = "";
    with open(path_csv, 'rb') as rawdata:
        encoding = chardet.detect(rawdata.read(1024))['encoding']
    dataframe = pd.read_csv(path_csv, sep=";", encoding=encoding, dtype=str)
    return dataframe['encounter_id']

def getAllOpsIds():
    path_csv = r"C:\Users\User\Desktop\ops\OPS.csv";
    encoding = "";
    with open(path_csv, 'rb') as rawdata:
        encoding = chardet.detect(rawdata.read(1024))['encoding']
    dataframe = pd.read_csv(path_csv, sep=";", encoding=encoding, dtype=str)
    return dataframe['KH-internes-Kennzeichen']

def getMatches():
    set_valid = set(getAllValidCases())
    set_ops = set(getAllOpsIds())
    return set_valid.intersection(set_ops)

def getMatchOrder():
    list_matches = list(getMatches())
    path_csv = r"C:\Users\User\Desktop\ops\OPS.csv";
    encoding = "";
    with open(path_csv, 'rb') as rawdata:
        encoding = chardet.detect(rawdata.read(1024))['encoding']
    dataframe = pd.read_csv(path_csv, sep=";", encoding=encoding, dtype=str)
    return dataframe
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 11 11:53:55 2021

@author: User
"""

import zipfile
import pandas as pd
import chardet


def check_csv_names(path_zip):
    with zipfile.ZipFile(path_zip, 'r') as file_zip:
        set_required_csv = set(DICT_P21_COLUMNS.keys())
        set_matched_csv = set_required_csv.intersection(set(file_zip.namelist()))
        if set_matched_csv != set_required_csv:
            raise SystemExit('following csv are missing in zip: {0}'.format(set_required_csv.difference(set_matched_csv)))
    print("csv are good")

def check_csv_column_headers(path_zip):
    with zipfile.ZipFile(path_zip, 'r') as file_zip:
        for name_csv in list(DICT_P21_COLUMNS.keys()):
            headers_csv = pd.read_csv(file_zip.open(name_csv, mode='r'), nrows=0, index_col=0, sep=CSV_SEPARATOR, encoding=chardet.detect(file_zip.read(name_csv))['encoding'])
            set_required_columns = set(DICT_P21_COLUMNS[name_csv])
            set_matched_columns = set_required_columns.intersection(set(headers_csv))
            if set_matched_columns != set_required_columns:
                raise SystemExit('following columns are missing in {0}: {1}'.format(name_csv, set_required_columns.difference(set_matched_columns)))
    print("columns are good")

def check_big_zip_encoding():
    path = r'C:\Users\User\IdeaProjects\aktin_python_scripts\p21_big.zip'
    with zipfile.ZipFile(path, 'r') as file_zip:
        for name_csv in file_zip.namelist():
            print(get_csv_encoding(file_zip, name_csv))

def get_csv_encoding(file_zip, name_csv):
    return chardet.detect(file_zip.open(name_csv).read(1024))['encoding']

if __name__ == '__main__':
    DICT_P21_COLUMNS = {
        'FALL.csv': ['KH-internes-Kennzeichen', 'IK-der-Krankenkasse', 'Geburtsjahr', 'Geschlecht', 'PLZ', 'Aufnahmedatum',
                     'Aufnahmegrund', 'Aufnahmeanlass', 'Fallzusammenführung', 'Fallzusammenführungsgrund', 'Verweildauer-intensiv',
                     'Entlassungsdatum', 'Entlassungsgrund', 'Beatmungsstunden', 'Behandlungsbeginn-vorstationär',
                     'Behandlungstage-vorstationär', 'Behandlungsende-nachstationär', 'Behandlungstage-nachstationär'],
        'FAB.csv': ['KH-internes-Kennzeichen', 'Fachabteilung', 'FAB-Aufnahmedatum', 'FAB-Entlassungsdatum', 'Kennung-Intensivbett'],
        'ICD.csv': ['KH-internes-Kennzeichen', 'Diagnoseart', 'ICD-Version', 'ICD-Kode', 'Lokalisation', 'Diagnosensicherheit',
                    'Sekundär-Kode', 'Sekundär-Lokalisation', 'Sekundär-Diagnosensicherheit'],
        'OPS.csv': ['KH-internes-Kennzeichen', 'OPS-Version', 'OPS-Kode', 'OPS-Datum', 'Lokalisation']
        }
    PATH = r'C:\Users\User\IdeaProjects\aktin_python_scripts\p21.zip'

    CSV_SEPARATOR = ';'
    CSV_ENCODER = 'latin1'
    CSV_CHUNKSIZE = 1000
    DB_CHUNKSIZE = 1000

    check_big_zip_encoding()
    #check_csv_column_headers(PATH)

# encoding='latin1', 'windows-1252'

"""
            with file_zip.open(name_csv, mode='r') as rawdata:
                result = chardet.detect(rawdata.read(10000))
                print(result)
"""
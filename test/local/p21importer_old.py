# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 11:36:55 2021
@author: akombeiz
"""
# @VERSION=1.2
# @VIEWNAME=p21import_test
# @MIMETYPE=zip
# @ID=p21import
"""
Script to verify and import p21 data into AKTIN DWH

AKTIN DWH calls one method of this script and provides path to zip-file.
Only the methods 'verify_file()' and 'import_file()' can be called by DWH.

verify_file() checks validity of given zip-file regarding p21 requirements
and matches valid encounters with found encounters in database.

import_file() iterates through matched encounters in fall.csv. All valid fields
of valid encounters are uploaded into the i2b2 database as observation_fact rows.
Prior uploading each encounter, it is checked if p21 data of encounter was
already uploaded using this script and deleted if necessary. After uploading
all encounter of fall.csv, the script iterates through the optional csv-files
(fab,icd,ops) and uploads their valid fields, too.
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
import psycopg2
import traceback
from datetime import datetime
import shutil

"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
VERIFY FILE
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""


def verify_file(path_zip):
    """
    Checks validity of a given zip-file regarding p21 requirements (path
    integrity, required column names, value formatting) and matches vaild
    encounters against optin encounters in i2b2. See docs of used methods for
    further details. Zip-File is unzipped into a temporary folder, where all
    files and csv-columns are renamed to their lowercase parts to provide
    case insensitivity.

    Parameters
    ----------
    path_zip : str
        Path to the zip-file

    Returns
    -------
    None.

    """
    try:
        engine = get_db_engine()
        with engine.connect() as connection:
            check_file_path_integrity(path_zip)
            path_tmp_folder = create_tmp_folder(path_zip)
            extract_zip_to_folder(path_zip, path_tmp_folder)
            prepare_import_folder(path_tmp_folder)
            list_valid = get_valid_FALL_encounter_ids(path_tmp_folder)
            list_matched = get_matched_FALL_encounter_ids(engine, connection, list_valid)
            num_total = count_total_FALL_encounter(path_tmp_folder)
            print_verification_results(num_total, list_valid, list_matched)
    finally:
        engine.dispose()
        remove_tmp_folder(path_zip)


def check_file_path_integrity(path_zip):
    """
    Checks, if file of given path exists and is a zip-file.

    Parameters
    ----------
    path_zip : str
        Path to the zip-file

    Raises
    ------
    SystemExit
        If path is invalid or file is not a zip-file

    Returns
    -------
    None.

    """
    if not os.path.exists(path_zip):
        raise SystemExit('file path is not valid')
    if not zipfile.is_zipfile(path_zip):
        raise SystemExit('file is not a zipfile')


def prepare_import_folder(path_folder):
    """
    Renames all files and columns of csv files in folder to lowercase. Raises
    Exceptions if necessary csv files or csv columns are missing.

    Parameters
    ----------
    path_folder : str
        Path to a folder with csv files

    Returns
    -------
    None.

    """
    rename_files_to_lowercase(path_folder)
    check_csv_names(path_folder)
    rename_csv_columns_to_lowercase(path_folder)
    check_csv_columns(path_folder)


def create_tmp_folder(path_zip):
    """
    Creates a temporary folder named 'tmp' in the parent folder of given path
    to zip file

    Parameters
    ----------
    path_zip : str
        Path to the zip-file

    Returns
    -------
    path_tmp_folder : str
        Path to the temporary folder

    """
    path_parent = os.path.dirname(path_zip)
    path_tmp_folder = os.path.join(path_parent, 'tmp')
    if not os.path.isdir(path_tmp_folder):
        os.makedirs(path_tmp_folder)
    return path_tmp_folder


def remove_tmp_folder(path_zip):
    """
    Removes (recursively) from the same folder as the given path to zip file a
    folder named tmp.

    Parameters
    ----------
    path_zip : str
        Path to the zip-file

    Returns
    -------
    None.

    """
    path_parent = os.path.dirname(path_zip)
    path_tmp_folder = os.path.join(path_parent, 'tmp')
    if os.path.isdir(path_tmp_folder):
        shutil.rmtree(path_tmp_folder)


def extract_zip_to_folder(path_zip, path_target):
    """
    Extracts a zip-file from given path to another given location

    Parameters
    ----------
    path_zip : str
        Path to the zip-file
    path_target : str
        Path where to extract zip

    Returns
    -------
    None.

    """
    with zipfile.ZipFile(path_zip, 'r') as file_zip:
        file_zip.extractall(path_target)


def rename_files_to_lowercase(path_folder):
    """
    Renames all files inside given folder to lowercase.

    Parameters
    ----------
    path_folder : str
       Path to a folder

    Returns
    -------
    None.

    """
    list_files_dir = [file for file in os.listdir(path_folder) if os.path.isfile(os.path.join(path_folder, file))]
    for file in list_files_dir:
        os.rename(os.path.sep.join([path_folder, file]), os.path.sep.join([path_folder, file.lower()]))


def check_csv_names(path_csv_folder):
    """
    Checks, if required file named 'fall.csv' exists within given folder.
    Checks existence of optional files named 'fab.csv', 'ops.csv' and 'icd.csv'.

    Parameters
    ----------
    path_csv_folder : str
        Path to the folder with csv files

    Raises
    ------
    SystemExit
        If fall.csv is missing

    Returns
    -------
    None.

    """
    set_required_csv = set(DICT_P21_COLUMNS.keys())
    set_files_dir = set([file for file in os.listdir(path_csv_folder) if os.path.isfile(os.path.join(path_csv_folder, file))])
    set_matched_csv = set_required_csv.intersection(set_files_dir)
    if 'fall.csv' not in set_matched_csv:
        raise SystemExit('fall.csv is missing in zip')
    if set_matched_csv != set_required_csv:
        print('following csv could not be found in zip: {0}'.format(set_required_csv.difference(set_matched_csv)))


def rename_csv_columns_to_lowercase(path_csv_folder):
    """
    Renames all columns of p21 csv files in given folder to lowercase.
    Case differntation for csv named 'icd.csv' is done, where possibly duplicate
    column headers (Lokalisation and Diagnosesicherheit for Sekundärdiagnose)
    have to be renamed. If csv file is not in folder, it is skipped.

    Parameters
    ----------
    path_folder : str
        Path to a folder.

    Returns
    -------
    None.

    """
    for name_csv in DICT_P21_COLUMNS.keys():
        path_csv = os.path.sep.join([path_csv_folder, name_csv])
        if not os.path.isfile(path_csv): continue
        path_tmp = os.path.sep.join([path_csv_folder, 'tmp.csv'])
        encoding = get_csv_encoding(path_csv)
        df = pd.read_csv(path_csv, nrows=0, index_col=None, sep=CSV_SEPARATOR, encoding=encoding, dtype=str)
        df.rename(columns=str.lower, inplace=True)
        if (name_csv == 'icd.csv'):
            df = add_term_to_ICD_columns(df)
        header_row = ';'.join(df.columns) + '\n'
        with open(path_csv, 'r+', encoding=encoding) as f1, open(path_tmp, 'w+', encoding=encoding) as f2:
            f1.readline()
            f2.write(header_row)
            shutil.copyfileobj(f1, f2)
        os.remove(path_csv)
        os.rename(path_tmp, path_csv)


def add_term_to_ICD_columns(df_icd):
    """
    Searches in dataframe for a column named 'sekundär-kode' and adds a
    'sekundär-' to following columns named 'lokalisation' and
    'diagnosensicherheit'.

    Parameters
    ----------
    df_icd : pandas.DataFrame
        Header dataframe of icd.csv

    Raises
    ------
    SystemExit
        If multiple columns named 'lokalisation' or 'diagnosensicherheit' were
        found after 'sekundär-kode'

    Returns
    -------
    df_icd : pandas.DataFrame
        Header dataframe of icd.csv with renamed columns for Sekundärdiagnosen

    """
    index = df_icd.columns.get_loc('sekundär-kode')
    array_bool = df_icd.columns[index:].str.match('^lokalisation(\.)?(\d*)?$')
    if (sum(array_bool) == 1):
        df_icd.rename(columns={df_icd.columns[index:][array_bool][0]: 'sekundär-lokalisation'}, inplace=True)
    elif (sum(array_bool) > 1):
        raise SystemExit('duplicate column for sekundär-lokalisation')
    array_bool = df_icd.columns[index:].str.match('^diagnosensicherheit(\.)?(\d*)?$')
    if (sum(array_bool) == 1):
        df_icd.rename(columns={df_icd.columns[index:][array_bool][0]: 'sekundär-diagnosensicherheit'}, inplace=True)
    elif (sum(array_bool) > 1):
        raise SystemExit('duplicate column for sekundär-diagnosensicherheit')
    return df_icd


def check_csv_columns(path_csv_folder):
    """
    Checks, if found csv-files ('fall.csv', 'fab.csv', 'ops.csv' and 'icd.csv')
    in given folder contain the required columns (see DICT_P21_COLUMNS). If
    csv file is not in folder, it is skipped.

    Parameters
    ----------
    path_csv_folder : str
        Path to the folder with csv files

    Raises
    ------
    SystemExit
        If one or more columns in csv-file are missing

    Returns
    -------
    None.

    """
    for name_csv in DICT_P21_COLUMNS.keys():
        path_csv = os.path.sep.join([path_csv_folder, name_csv])
        if not os.path.isfile(path_csv): continue
        df = pd.read_csv(path_csv, nrows=0, index_col=None, sep=CSV_SEPARATOR, encoding=get_csv_encoding(path_csv), dtype=str)
        set_required_columns = set(DICT_P21_COLUMNS[name_csv])
        set_matched_columns = set_required_columns.intersection(set(df.columns))
        if set_matched_columns != set_required_columns:
            raise SystemExit('following columns are missing in {0}: {1}'.format(name_csv, set_required_columns.difference(set_matched_columns)))


def get_valid_FALL_encounter_ids(path_csv_folder):
    """
    Checks all columns in fall.csv for formatting requirements and returns a
    list of valid encounter ids

    Parameters
    ----------
    path_csv_folder : str
        Path to folder with fall.csv

    Raises
    ------
    SystemExit
        If no valid encounter could be found

    Returns
    -------
    list_valid_FALL_encounter : list
        List of encounter ids where all fields abide the formatting
        requirements in fall.csv

    """
    path_csv_FALL = os.path.join(path_csv_folder, 'fall.csv')
    list_valid_FALL_encounter = get_valid_encounter_ids(path_csv_FALL)
    if not list_valid_FALL_encounter:
        raise SystemExit('no valid encounter found in fall.csv')
    return list_valid_FALL_encounter


def get_valid_encounter_ids(path_csv):
    """
    Iterates in chunks trough a given csv-file and checks each column in chunk
    for empty fields or wrong value formatting. Encounter ids of fields which
    do not meet format criteria (see DICT_P21_COLUMN_PATTERN and
    LIST_P21_COLUMN_MANDATORY) are removed and excluded from further processing

    Parameters
    ----------
    path_csv : str
        Path to the csv file

    Returns
    -------
        List of encounter ids in csv, where all columns follow the p21 requirements

    """
    set_valid_enc = set()
    name_csv = os.path.basename(path_csv)
    for chunk in pd.read_csv(path_csv, chunksize=CSV_CHUNKSIZE, sep=CSV_SEPARATOR, encoding=get_csv_encoding(path_csv), dtype=str):
        chunk = chunk[DICT_P21_COLUMNS[name_csv]].fillna('')
        for column in DICT_P21_COLUMNS[name_csv]:
            chunk = clear_invalid_fields_in_chunk(chunk, column);
        set_valid_enc.update(chunk['kh-internes-kennzeichen'].unique())
    return list(set_valid_enc)


def clear_invalid_fields_in_chunk(chunk, column):
    """
    Checks a given column of given chunk of csv file for invalid fields.
    If a mandatory field has an invalid or empty value, the respective row
    is dropped. An invalid value in an optional field is cleared (field will
    not be imported).

    Parameters
    ----------
    chunk : pandas.DataFrame
        DataFrame chunk of csv file.
    column : str
        Name of Dataframe column to check fields in

    Returns
    -------
    chunk : pandas.DataFrame
        DataFrame chunk from input, but with dropped rows/cleared fields

    """
    pattern = DICT_P21_COLUMN_PATTERN[column]
    indeces_empty_fields = chunk[chunk[column] == ''].index
    indeces_wrong_syntax = chunk[(chunk[column] != '') & (chunk[column].str.match(pattern) == False)].index
    if len(indeces_wrong_syntax):
        if column not in LIST_P21_COLUMN_MANDATORY:
            chunk.at[indeces_wrong_syntax, column] = ''
        else:
            chunk = chunk.drop(indeces_wrong_syntax)
    if len(indeces_empty_fields) and column in LIST_P21_COLUMN_MANDATORY:
        chunk = chunk.drop(indeces_empty_fields)
    return chunk


def get_matched_FALL_encounter_ids(engine, connection, list_FALL_encounter):
    """
    Matches a list of encounter ids from fall.csv against optin encounters in
    database.

    Parameters
    ----------
    engine : sqlalchemy.engine
        Engine object of get_db_engine()
    connection : sqlalchemy.connection
        Connection object of engine to run querys on
    list_FALL_encounter : list
        List with encounter ids from fall.csv

    Raises
    ------
    SystemExit
        If no encounter matches with encounter in database

    Returns
    -------
    list_matched_FALL_encounter : list
        List with matched encounter ids from fall.csv

    """
    list_matched_FALL_encounter = get_matched_encounter_ids(engine, connection, list_FALL_encounter)
    if not list_matched_FALL_encounter:
        raise SystemExit('no encounter in fall.csv could be matched with database')
    return list_matched_FALL_encounter


def get_matched_encounter_ids(engine, connection, list_encounter):
    """
    Matches a given list of encounter ids against optin encounters in
    database. List of encounter ids has to be hashed to allow a matching.

    Parameters
    ----------
    engine : sqlalchemy.engine
        Engine object of get_db_engine()
    connection : sqlalchemy.connection
        Connection object of engine to run querys on
    list_encounter : list
        List with encounter ids to match with database

    Returns
    -------
    list
        List with matched (and hashed) encounter ids

    """
    df_db_enc = get_AKTIN_optin_encounter_df(engine, connection)
    list_db_ide = df_db_enc['encounter_ide'].tolist()
    list_enc_ide = anonymize_enc_list(list_encounter)
    return list(set(list_enc_ide).intersection(set(list_db_ide)))


def get_AKTIN_optin_encounter_df(engine, connection):
    """
    Runs a query on i2b2crcdata for all encounters in encounter_mapping
    where the corresponding patients do not appear in optinout_patients of
    AKTIN (either patients without pat_psn or patients with pat_psn, but without
    study_id = 'AKTIN'). Queries for encounter_ide and corresponding
    patient_num and encounter_num. Streams query results into a DataFrame.

    Parameters
    ----------
    engine : sqlalchemy.engine
        Engine object of get_db_engine()
    connection : sqlalchemy.connection
        Connection object of engine to run querys on

    Returns
    -------
    df_result : pandas.DataFrame
        DataFrame with all (hashed) encounter ids which are not marked as optout
        for AKTIN study and corresponding encounter_num and patient_num

    """
    enc = db.Table('encounter_mapping', db.MetaData(), autoload_with=engine)
    pat = db.Table('patient_mapping', db.MetaData(), autoload_with=engine)
    opt = db.Table('optinout_patients', db.MetaData(), autoload_with=engine)
    query = db.select([enc.c['encounter_ide'], enc.c['encounter_num'], pat.c['patient_num']]).select_from(
        enc.join(pat, enc.c['patient_ide'] == pat.c['patient_ide']).join(opt, pat.c['patient_ide'] == opt.c['pat_psn'], isouter=True)).where(
        db.or_(opt.c['study_id'] != 'AKTIN', opt.c['pat_psn'].is_(None)))
    df_result = stream_query_into_df(connection, query)
    return df_result


def stream_query_into_df(connection, query):
    """
    Runs a given query on a given connection and streams result into a DataFrame.
    Only useage is to stream (possibly large) results of
    get_AKTIN_optin_pat_and_enc()

    Parameters
    ----------
    connection : sqlalchemy.connection
        Connection object of engine to run querys on
    query : sqlalchemy.Select
        Sqlalchemy query object

    Returns
    -------
    df_result : pandas.DataFrame
        DataFrame with results of executed query

    """
    df_result = pd.DataFrame()
    result = connection.execution_options(stream_results=True).execute(query)
    while True:
        chunk = result.fetchmany(DB_CHUNKSIZE)
        if not chunk:
            break
        if df_result.empty:
            df_result = pd.DataFrame(chunk)
        else:
            df_result = df_result.append(chunk, ignore_index=True)
    print(df_result.empty)
    print(df_result)
    df_result.columns = result.keys()
    return df_result


def count_total_FALL_encounter(path_csv_folder):
    """
    Counts the total number of encounter in fall.csv (each row is an individual
    encounter)

    Parameters
    ----------
    path_csv_folder : str
        Path to folder with fall.csv

    Returns
    -------
    int
        Number of encounter in fall.csv

    """
    path_csv_FALL = os.path.join(path_csv_folder, 'fall.csv')
    return count_csv_rows(path_csv_FALL)


def count_csv_rows(path_csv):
    """
    Counts the rows of a given csv file (excluding header)

    Parameters
    ----------
    path_csv : str
        Path to a csv file

    Returns
    -------
    int
        Number of rows in csv file

    """
    with open(path_csv, encoding=get_csv_encoding(path_csv)) as csv:
        total = sum(1 for row in csv)
    return total - 1


def print_verification_results(num_total, list_valid, list_matched):
    """
    Prints verification results about valid and matched encounter in console

    Parameters
    ----------
    num_total : int
        Number of total encounter
    list_valid : list
        List with valid encounter ids
    list_matched : list
        List with database matched encounter ids

    Returns
    -------
    None.

    """
    print('Fälle gesamt: %d' % num_total)
    print('Fälle valide: %d' % len(list_valid))
    print('Valide Fälle gematcht mit Datenbank: %d' % len(list_matched))


"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
IMPORT FILE
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""


def import_file(path_zip):
    """
    Imports p21 data of all valid and database matched encounters of
    fall.csv to i2b2crcdata.observation_fact. Checks, if p21 data of encounters
    were already uploaded using this script and deletes entries if necessary.
    Imports optional encounter data from other csv files afterwards.

    TODO: Refactor this monstrosity
    TODO: outsource 'aufnahmedatum' from df_match

    Parameters
    ----------
    path_zip : str
        Path to the zip-file

    Returns
    -------
    None.

    """
    try:
        num_imports = 0
        num_updates = 0
        engine = get_db_engine()
        with engine.connect() as connection:
            check_file_path_integrity(path_zip)
            path_tmp_folder = create_tmp_folder(path_zip)
            extract_zip_to_folder(path_zip, path_tmp_folder)
            prepare_import_folder(path_tmp_folder)
            list_valid_encounter = get_valid_FALL_encounter_ids(path_tmp_folder)
            df_match = get_database_matched_encounter_df(engine, connection, list_valid_encounter)
            table_observation = db.Table('observation_fact', db.MetaData(), autoload_with=engine)

            for name_csv in DICT_P21_COLUMNS.keys():
                path_csv = os.path.sep.join([path_tmp_folder, name_csv])
                if not os.path.isfile(path_csv): continue
                map_num_instances = {}
                for chunk in pd.read_csv(path_csv, chunksize=CSV_CHUNKSIZE, sep=CSV_SEPARATOR, encoding=get_csv_encoding(path_csv), dtype=str):
                    chunk = chunk[chunk['kh-internes-kennzeichen'].isin(df_match['encounter_id'])]
                    if chunk.empty:
                        continue
                    chunk = chunk[DICT_P21_COLUMNS[name_csv]].fillna('')
                    for column in DICT_P21_COLUMNS[name_csv]:
                        chunk = clear_invalid_fields_in_chunk(chunk, column)
                    list_encounter_data_upload = []
                    for row_chunk in chunk.iterrows():
                        list_row_upload = []
                        row_chunk = row_chunk[1]
                        num_enc, num_pat = get_enc_nums_from_df(row_chunk['kh-internes-kennzeichen'], df_match)
                        index_enc = df_match[df_match['encounter_id'] == row_chunk['kh-internes-kennzeichen']].index
                        if name_csv == 'fall.csv':
                            date_admission = row_chunk['aufnahmedatum']
                            df_match.at[index_enc, 'aufnahmedatum'] = date_admission
                        else:
                            date_admission = df_match.iloc[index_enc]['aufnahmedatum'].values[0]
                        if name_csv == 'fall.csv':
                            if check_if_encounter_is_imported(connection, table_observation, num_enc):
                                delete_encounter_data(connection, table_observation, num_enc)
                                num_updates += 1
                            else:
                                num_imports += 1
                            list_row_upload = insert_upload_data_FALL(row_chunk)
                            list_row_upload.extend(create_script_rows())
                        elif name_csv == 'fab.csv':
                            list_row_upload, map_num_instances = insert_upload_data_fab(row_chunk, map_num_instances)
                        elif name_csv == 'ops.csv':
                            list_row_upload, map_num_instances = insert_upload_data_ops(row_chunk, map_num_instances)
                        elif name_csv == 'icd.csv':
                            list_row_upload, map_num_instances = insert_upload_data_icd(row_chunk, date_admission, map_num_instances)
                        for index, row_upload in enumerate(list_row_upload):
                            list_row_upload[index] = add_fixed_values(row_upload, num_enc, num_pat, date_admission)
                        list_encounter_data_upload.extend(list_row_upload)
                    upload_encounter_data(connection, table_observation, list_encounter_data_upload)
            print_import_results(num_imports, num_updates)
    finally:
        engine.dispose()
        remove_tmp_folder(path_zip)


def get_database_matched_encounter_df(engine, connection, list_encounter):
    """
    Compares list of encounter ids with optin encounter ids of
    i2b2crcdata and returns DataFrame with matched encounter and corresponding
    patient_num and encounter_num. Adds also an empty column named
    aufnahmedatum to write admission date into (is written in from fall.csv and
    read afterwards by optional CSV files)

    Parameters
    ----------
    engine : sqlalchemy.engine
        Engine object of get_db_engine()
    connection : sqlalchemy.connection
        Connection object of engine to run querys on
    list_encounter : list
        List with encounter ids to match with encounter in database

    Returns
    -------
    df_merged : pandas.DataFrame
        DataFrame with matched unhashed encounter ids and corresponding
        patient_num and encounter_num

    """
    df_db_enc = get_AKTIN_optin_encounter_df(engine, connection)
    list_enc_ide = anonymize_enc_list(list_encounter)
    df_enc_ide = pd.DataFrame(list(zip(list_encounter, list_enc_ide)), columns=['encounter_id', 'encounter_ide'])
    df_merged = pd.merge(df_db_enc, df_enc_ide, on=['encounter_ide'])
    df_merged = df_merged.drop(['encounter_ide'], axis=1)
    df_merged['aufnahmedatum'] = ''
    return df_merged


def get_enc_nums_from_df(id_encounter, df_match):
    """
    Extracts from df_match corresponding encounter_num and patient_num for
    given encounter id

    Parameters
    ----------
    id_encounter : str
        Encounter id to get corresponding encounter_num and patient_num from
    df_match : pandas.DataFrame
        DataFrame with matched unhashed encounter ids and corresponding
        patient_num and encounter_num

    Returns
    -------
    num_encounter : str
        Encounter_num of encounter
    num_patient : str
        Patient_num of encounter

    """
    num_encounter = int(df_match.loc[df_match['encounter_id'] == id_encounter]['encounter_num'].iloc[0])
    num_patient = int(df_match.loc[df_match['encounter_id'] == id_encounter]['patient_num'].iloc[0])
    return num_encounter, num_patient


def insert_upload_data_FALL(row_FALL):
    """
    Converts p21 variables from fall.csv of given row into a list of
    i2b2crcdata.observation_fact rows. Only mandatory column values are
    created for each row. Default values (like provider_id or sourcesystem_cd)
    are added prior upload through add_fixed_values(). Keeps track of multiple
    appearances of encounters through map_num_instances and enumerates
    corresponding num_instance

    Notes:
        In fall.csv, only the columns 'aufnahmedatum','aufnahmegrund' and
        'aufnahmeanlass' are mandatory

        Other columns may be empty and are only added, if the columns contains
        a value

        Columns 'entlassungsdatum' and 'entlassungsgrund' are only added, if
        both columns contain a value

        Columns 'fallzusammenführung' and 'fallzusammenführungsgrund' are only
        added, if both columns contain a value and 'fallzusammenführung' equals
        'J'

        Column 'behandlungstage-vorstationär' is only added, if
        'behandlungsbeginn-vorstationär' contains a value, but is not mandatory
        for 'behandlungsbeginn-vorstationär' to be added

        Same goes for 'behandlungstage-nachstationär' with
        'behandlungsende-nachstationär'

    Parameters
    ----------
    row : pandas.Series
        Single row of fall.csv chunk to convert into observation_fact rows

    Returns
    -------
    list_observation_dicts : list
        List of observation_fact rows of fall.csv data

    """
    list_observation_dicts = []
    list_observation_dicts.extend(create_rows_admission(row_FALL['aufnahmeanlass'], row_FALL['aufnahmegrund']))
    if row_FALL['ik-der-krankenkasse']:
        list_observation_dicts.append(create_row_insurance(row_FALL['ik-der-krankenkasse']))
    if row_FALL['geburtsjahr']:
        list_observation_dicts.extend(create_rows_birthyear(row_FALL['geburtsjahr'], row_FALL['aufnahmedatum']))
    if row_FALL['geschlecht']:
        list_observation_dicts.append(create_row_sex(row_FALL['geschlecht']))
    if row_FALL['plz']:
        list_observation_dicts.append(create_row_zipcode(row_FALL['plz']))
    if row_FALL['fallzusammenführung'] == 'J' and row_FALL['fallzusammenführungsgrund']:
        list_observation_dicts.append(create_row_encounter_merge(row_FALL['fallzusammenführungsgrund']))
    if row_FALL['verweildauer-intensiv']:
        list_observation_dicts.append(create_row_critical_care(row_FALL['verweildauer-intensiv']))
    if row_FALL['entlassungsdatum'] and row_FALL['entlassungsgrund']:
        list_observation_dicts.append(create_row_discharge(row_FALL['entlassungsdatum'], row_FALL['entlassungsgrund']))
    if row_FALL['beatmungsstunden']:
        list_observation_dicts.append(create_row_ventilation(row_FALL['beatmungsstunden']))
    if row_FALL['behandlungsbeginn-vorstationär']:
        list_observation_dicts.append(create_row_therapy_start_prestation(row_FALL['behandlungsbeginn-vorstationär'], row_FALL['behandlungstage-vorstationär']))
    if row_FALL['behandlungsende-nachstationär']:
        list_observation_dicts.append(create_row_therapy_end_poststation(row_FALL['behandlungsende-nachstationär'], row_FALL['behandlungstage-nachstationär']))
    return list_observation_dicts


def create_rows_admission(cause, reason):
    """
    Creates observation_fact rows for encounter admission. Cause and reason
    are both added as seperate rows with seperate concept_cd.

    Parameters
    ----------
    cause : str
        Value of 'aufnahmeanlass' in fall.csv
    reason : str
        Value of 'aufnahmegrund' in fall.csv

    Returns
    -------
    list
        List with dicts (observation_fact rows) for encounter admission

    """
    concept_cause = ':'.join(['P21:ADMC', str.upper(cause)])
    concept_reason = ':'.join(['P21:ADMR', str.upper(reason)])
    return [{'concept_cd': concept_cause, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'},
            {'concept_cd': concept_reason, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}]


def create_row_insurance(insurance):
    """
    Creates observation_fact row for insurance number of encounter

    Parameters
    ----------
    insurance : str
        Value of 'ik-der-krankenkasse' in fall.csv

    Returns
    -------
    dict
        Observation_fact row for insurance number

    """
    return {'concept_cd': 'AKTIN:IKNR', 'modifier_cd': '@', 'valtype_cd': 'T', 'tval_char': insurance}


def create_rows_birthyear(birthyear, date_admission):
    """
    Creates observation_fact rows for encounter birthyear. LOINC code is used
    as concept_cd

    Parameters
    ----------
    birthyear : str
        Value of 'geburtsjahr' in fall.csv
    date_admission : str
        Admission date of encounter (%Y%m%d%H%M). Is saved unformatted as
        modifier 'effectiveTime'

    Returns
    -------
    list
        List with dicts (observation_fact rows) for encounter birthyear

    """
    return [{'concept_cd': 'LOINC:80904-6', 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': birthyear, 'units_cd': 'yyyy'},
            {'concept_cd': 'LOINC:80904-6', 'modifier_cd': 'effectiveTime', 'valtype_cd': 'T', 'tval_char': date_admission}]


def create_row_sex(sex):
    """
    Creates observation_fact row for sex of encounter patient. Patient sex isa
    dded as a concept_cd

    Parameters
    ----------
    sex : str
        Value of 'geschlecht' in fall.csv

    Returns
    -------
    dict
        Observation_fact row for patient sex

    """
    concept_sex = ':'.join(['P21:SEX', str.upper(sex)])
    return {'concept_cd': concept_sex, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}


def create_row_zipcode(zipcode):
    """
    Creates observation_fact row for zipcode of encounter patient

    Parameters
    ----------
    zipcode : str
         Value of 'plz' in fall.csv

    Returns
    -------
    dict
        Observation_fact row for patient zipcode

    """
    return {'concept_cd': 'AKTIN:ZIPCODE', 'modifier_cd': '@', 'valtype_cd': 'T', 'tval_char': zipcode}


def create_row_encounter_merge(reason):
    """
    Creates observation_fact row for reason of encounter merge (row is only
    created, if encounter merge occured, as no merge == no reason)

    Parameters
    ----------
    val_merge : str
         Value of 'fallzusammenführungsgrund' in fall.csv

    Returns
    -------
    dict
        Observation_fact row for encounter merge

    """
    concept_merge = ':'.join(['P21:MERGE', str.upper(reason)])
    return {'concept_cd': concept_merge, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}


def create_row_critical_care(intensive):
    """
    Creates observation_fact row for stayed duration in critical care of
    encounter patient

    Parameters
    ----------
    intensive : str
        Value of 'verweildauer-intensiv' in fall.csv

    Returns
    -------
    dict
       Observation_fact row for duration in critical care

    """
    intensive = intensive.replace(',', '.')
    return {'concept_cd': 'P21:DCC', 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': intensive, 'units_cd': 'd'}


def create_row_discharge(date_end, reason):
    """
    Creates observation_fact row for encounter discharge. Reason is added as
    concept_cd. End date has to be formatted in '%Y-%m-%d %H:%M'

    Parameters
    ----------
    date_end : str
        Value of 'entlassungsdatum' in fall.csv (%Y%m%d%H%M)
    reason : str
        Value of 'entlassungsgrund' in fall.csv

    Returns
    -------
    list
        List with dicts (observation_fact rows) for encounter discharge

    """
    date_end = convert_date_to_i2b2_format(date_end)
    concept_reason = ':'.join(['P21:DISR', str.upper(reason)])
    return {'concept_cd': concept_reason, 'start_date': date_end, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}


def create_row_ventilation(ventilation):
    """
    Creates observation_fact row for duration of respiratory ventilation of
    encounter patient

    Parameters
    ----------
    ventilation : str
        Value of 'beamtungsstunden' in fall.csv

    Returns
    -------
    dict
        Observation_fact row for duration of ventilation

    """
    ventilation = ventilation.replace(',', '.')
    return {'concept_cd': 'P21:DV', 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': ventilation, 'units_cd': 'h'}


def create_row_therapy_start_prestation(date_start, days):
    """
    Creates observation_fact rows for prestationary therapy start of encounter.
    Start date has to be converted in '%Y-%m-%d %H:%M'. Hours and minutes are
    added as 00:00. Variable days is optional and will be not inserted as
    row value if empty.

    Parameters
    ----------
    date_start : str
        Value of 'behandlungsbeginn-vorstationär' in fall.csv (%Y%m%d)
    days : str
        Value of 'behandlungstage-vorstationär' in fall.csv

    Returns
    -------
    result : dict
        Observation_fact row for prestationary therapy start

    """
    date_start = convert_date_to_i2b2_format(''.join([date_start, '0000']))
    if days:
        result = {'concept_cd': 'P21:PREADM', 'start_date': date_start, 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': days, 'units_cd': 'd'}
    else:
        result = {'concept_cd': 'P21:PREADM', 'start_date': date_start, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}
    return result


def create_row_therapy_end_poststation(date_end, days):
    """
    Creates observation_fact rows for poststaionary therapy end of encounter.
    End date has to be converted in '%Y-%m-%d %H:%M'. Hours and minutes are
    added as 00:00. Variable days is optional and will be not inserted as
    row value if empty.

    Parameters
    ----------
    date_end : str
        Value of 'behandlungsende-nachstationär' in fall.csv (%Y%m%d)
    days : str
        Value of 'behandlungstage-nachstationär' in fall.csv

    Returns
    -------
    result : dict
        Observation_fact row for  poststationary therapy end

    """
    date_end = convert_date_to_i2b2_format(''.join([date_end, '0000']))
    if days:
        result = {'concept_cd': 'P21:POSTDIS', 'start_date': date_end, 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': days, 'units_cd': 'd'}
    else:
        result = {'concept_cd': 'P21:POSTDIS', 'start_date': date_end, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}
    return result


def insert_upload_data_fab(row, map_num_instances):
    """
    Converts p21 variables from fab.csv of given row into a list of
    i2b2crcdata.observation_fact rows. Only mandatory column values are
    created for each row. Default values (like provider_id or sourcesystem_cd)
    are added prior upload through add_fixed_values(). Keeps track of multiple
    appearances of encounters through map_num_instances and enumerates
    corresponding num_instance

    Parameters
    ----------
    row : pandas.Series
        Single row of fab.csv chunk to convert into observation_fact rows
    map_num_instances : map
        Map to keep track of instance number of reappearing encounter

    Returns
    -------
    list_observation_dicts : list
        List of observation_fact rows of fab.csv data

    """
    map_num_instances = count_instance_num(row, map_num_instances)
    num_instance = map_num_instances.get(row['kh-internes-kennzeichen'])
    return create_row_department(num_instance, row['fachabteilung'], row['kennung-intensivbett'], row['fab-aufnahmedatum'], row['fab-entlassungsdatum']), map_num_instances


def create_row_department(num_instance, department, intensive, date_start, date_end):
    """
    Creates observation_fact rows for one stay in a certain department of
    encounter patient. End date and start date have to be formatted in
    '%Y-%m-%d %H:%M'. End date is not mandatory and will be written as None
    if empty

    Parameters
    ----------
    num_instance : int
        Instance_num in observation_fact
    department : str
        Value of 'fachabteilung' in fab.csv
    intensive : str
        Value of 'kennung-intensivbett' in fab.csv
    date_start : str
        Value of 'fab-aufnahmedatum' in fab.csv (%Y%m%d%H%M)
    date_end : str
        Value of 'fab-entlassungsdatum' in fab.csv (%Y%m%d%H%M)

    Returns
    -------
    dict
        Observation fact row for stay in given department of encounter

    """
    date_start = convert_date_to_i2b2_format(date_start)
    date_end = convert_date_to_i2b2_format(date_end) if date_end else None
    concept_dep = 'P21:DEP:CC' if intensive == 'J' else 'P21:DEP'
    return [{'concept_cd': concept_dep, 'start_date': date_start, 'modifier_cd': '@', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': department, 'end_date': date_end}]


def insert_upload_data_ops(row, map_num_instances):
    """
    Converts p21 variables from ops.csv of given row into a list of
    i2b2crcdata.observation_fact rows. Only mandatory column values are
    created for each row. Default values (like provider_id or sourcesystem_cd)
    are added prior upload through add_fixed_values(). Keeps track of multiple
    appearances of encounters through map_num_instances and enumerates
    corresponding num_instance

    Parameters
    ----------
    row : pandas.Series
        Single row of ops.csv chunk to convert into observation_fact rows
    map_num_instances : map
        Map to keep track of instance number of reappearing encounter

    Returns
    -------
    list_observation_dicts : list
        List of observation_fact rows of ops.csv data

    """
    map_num_instances = count_instance_num(row, map_num_instances)
    num_instance = map_num_instances.get(row['kh-internes-kennzeichen'])
    return create_rows_ops(num_instance, row['ops-kode'], row['ops-version'], row['lokalisation'], row['ops-datum']), map_num_instances


def create_rows_ops(num_instance, code_ops, version, localisation, date_ops):
    """
    Creates observation_fact rows for carried out procedures on encounter
    patient. The ops code itself is added as a concept_cd. OPS date has to be
    formatted in '%Y-%m-%d %H:%M'. Variable named localisation is optional and
    its row creation is skipped if localisation is empty.

    Parameters
    ----------
    num_instance : int
        Instance_num in observation_fact
    code_ops : str
        Value of 'ops-kode' in ops.csv
    version : str
        Value of 'ops-version' in ops.csv
    localisation : str
        Value of 'lokalisation' in ops.csv
    date_ops : str
        Value of 'ops-datum' in ops.csv (%Y%m%d%H%M)

    Returns
    -------
    result : list
         List with dicts (observation_fact rows) for carried out procedures

    """
    date_ops = convert_date_to_i2b2_format(date_ops)
    concept_ops = ':'.join(['OPS', convert_ops_code_to_i2b2_format(code_ops)])
    result = [{'concept_cd': concept_ops, 'start_date': date_ops, 'modifier_cd': '@', 'instance_num': num_instance, 'valtype_cd': '@', 'valueflag_cd': '@'},
              {'concept_cd': concept_ops, 'start_date': date_ops, 'modifier_cd': 'cdVersion', 'instance_num': num_instance, 'valtype_cd': 'N', 'nval_num': version, 'units_cd': 'yyyy'}]
    if localisation:
        result.append({'concept_cd': concept_ops, 'start_date': date_ops, 'modifier_cd': 'localisation', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': localisation})
    return result


def insert_upload_data_icd(row, date_admission, map_num_instances):
    """
    Converts p21 variables from icd.csv of given row into a list of
    i2b2crcdata.observation_fact rows. Only mandatory column values are
    created for each row. Default values (like provider_id or sourcesystem_cd)
    are added prior upload through add_fixed_values(). Keeps track of multiple
    appearances of encounters through map_num_instances and enumerates
    corresponding num_instance

    Parameters
    ----------
    row : pandas.Series
        Single row of ops.csv chunk to convert into observation_fact rows
    map_num_instances : map
        Map to keep track of instance number of reappearing encounter

    Returns
    -------
    list_observation_dicts : list
        List of observation_fact rows of ops.csv data

    """
    list_observation_dicts = []
    map_num_instances = count_instance_num(row, map_num_instances)
    num_instance = map_num_instances.get(row['kh-internes-kennzeichen'])
    list_observation_dicts.extend(create_rows_icd(num_instance, row['icd-kode'], row['diagnoseart'], row['icd-version'], row['lokalisation'], row['diagnosensicherheit'], date_admission))

    map_num_instances = count_instance_num(row, map_num_instances)
    num_instance = map_num_instances.get(row['kh-internes-kennzeichen'])
    list_observation_dicts.extend(create_row_icd_sek(num_instance, row['sekundär-kode'], row['icd-kode'], row['icd-version'], row['sekundär-lokalisation'], row['sekundär-diagnosensicherheit'], date_admission)) if row['sekundär-kode'] else None
    return list_observation_dicts, map_num_instances


def create_rows_icd(num_instance, code_icd, diag_type, version, localisation, certainty, date_adm):
    """
    Creates observation_fact rows for an encounter diagnosis. The icd code
    itself is added as a concept_cd. Variables named localisation and certainty
    are optional and row creation is skipped if these are empty.

    Parameters
    ----------
    num_instance : int
        Instance_num in observation_fact
    code_icd : str
        Value of 'icd-kode' in icd.csv
    diag_type : str
        Value of 'diagnoseart' in icd.csv
    version : str
        Value of 'icd-version' in icd.csv
    localisation : str
        Value of 'lokalisation' in icd.csv
    certainty : str
        Value of 'diagnosensicherheit' in icd.csv
    date_adm : str
        Admission date of encounter (%Y%m%d%H%M). Is safed unformatted
        as modifier 'EffectiveTimeLow'

    Returns
    -------
    result : list
         List with dicts (observation_fact rows) for given diagnosis

    """
    concept_icd = ':'.join(['ICD10GM', convert_icd_code_to_i2b2_format(code_icd)])
    result = [{'concept_cd': concept_icd, 'modifier_cd': '@', 'instance_num': num_instance, 'valtype_cd': '@', 'valueflag_cd': '@'},
              {'concept_cd': concept_icd, 'modifier_cd': 'diagType', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': diag_type},
              {'concept_cd': concept_icd, 'modifier_cd': 'cdVersion', 'instance_num': num_instance, 'valtype_cd': 'N', 'nval_num': version, 'units_cd': 'yyyy'},
              {'concept_cd': concept_icd, 'modifier_cd': 'effectiveTimeLow', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': date_adm}]
    if localisation:
        result.append({'concept_cd': concept_icd, 'modifier_cd': 'localisation', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': localisation})
    if certainty:
        result.append({'concept_cd': concept_icd, 'modifier_cd': 'certainty', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': certainty})
    return result


def create_row_icd_sek(num_instance, code_icd, code_parent, version, localisation, certainty, date_adm):
    """
    Creates observation_fact rows for a Sekundärdiagnose of an encounter.
    Each Sekundärdiagnose must have a corresponding Hauptdiagnose. Calls
    create_rows_icd(), but with diag_type='SD' and adds an own sdFrom-modifier

    Parameters
    ----------
    num_instance : int
        Instance_num in observation_fact
    code_icd : str
        Value of 'sekundär-kode' in icd.csv
    code_parent : str
        Value of 'icd-kode' in icd.csv
    version : str
        Value of 'icd-version' in icd.csv
    localisation : str
        Value of 'sekundär-lokalisation' in icd.csv
    certainty : str
        Value of 'sekundär-diagnosensicherheit' in icd.csv
    date_adm : str
        Admission date of encounter (%Y%m%d%H%M). Is safed unformatted
        as modifier 'EffectiveTimeLow'

    Returns
    -------
    result : list
         List with dicts (observation_fact rows) for given sekundärdiagnose

    """
    result = create_rows_icd(num_instance, code_icd, 'SD', version, localisation, certainty, date_adm)
    concept_parent = ':'.join(['ICD10GM', convert_icd_code_to_i2b2_format(code_parent)])
    concept_icd = ':'.join(['ICD10GM', convert_icd_code_to_i2b2_format(code_icd)])
    result.append({'concept_cd': concept_icd, 'modifier_cd': 'sdFrom', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': concept_parent})
    return result


def count_instance_num(row, map_num_instances):
    """
    Keeps track of reappearing encounter data. Enumerates instance number if
    encounter_id of given row was already given to this method at earlier time.
    Adds entry for encounter in map if encounter id is new

    Parameters
    ----------
    row : pandas.Series
        Row of chunk to convert into observation_fact rows
    map_num_instances : map
        Map with encounter_id : current instance num

    Returns
    -------
    map_num_instances : map

    """

    if row['kh-internes-kennzeichen'] not in map_num_instances:
        map_num_instances[row['kh-internes-kennzeichen']] = 1
    else:
        map_num_instances[row['kh-internes-kennzeichen']] += 1
    return map_num_instances


def create_script_rows():
    """
    Creates observation_fact rows for script metadata (input from environment
    variables)

    Returns
    -------
    list
        List with dicts (observation_fact rows) for script metadata

    """

    return [{'concept_cd': 'P21:SCRIPT', 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'},
            {'concept_cd': 'P21:SCRIPT', 'modifier_cd': 'scriptVer', 'valtype_cd': 'T', 'tval_char': SCRIPT_VERSION},
            {'concept_cd': 'P21:SCRIPT', 'modifier_cd': 'scriptId', 'valtype_cd': 'T', 'tval_char': SCRIPT_ID}]


def add_fixed_values(dict_row, num_enc, num_pat, date_adm):
    """
    Adds static values to a single observation_fact row and checks, if all
    columns were created for the row. Adds required columns with default values
    if necessary

    Parameters
    ----------
    dict_row : dict
        Observation_fact row to add fixed values to
    num_enc : str
        Encounter_num of encounter
    num_pat : str
        Patient_num of encounter
    date_adm : str
        Admission date of encounter (%Y-%m-%d %H:%M)
    date_import : str
        Current Timestamp (%Y-%m-%d %H:%M)

    Returns
    -------
    dict_row : dict
        Observation_fact row with added required columns

    """
    date_import = datetime.now(tz=None).strftime('%Y-%m-%d %H:%M:%S.%f')
    date_adm = convert_date_to_i2b2_format(date_adm)

    dict_row['encounter_num'] = num_enc
    dict_row['patient_num'] = num_pat
    dict_row['provider_id'] = 'P21'
    if 'start_date' not in dict_row:
        dict_row['start_date'] = date_adm
    if 'instance_num' not in dict_row:
        dict_row['instance_num'] = 1
    if 'tval_char' not in dict_row:
        dict_row['tval_char'] = None
    if 'nval_num' not in dict_row:
        dict_row['nval_num'] = None
    if 'valueflag_cd' not in dict_row:
        dict_row['valueflag_cd'] = None
    if 'units_cd' not in dict_row:
        dict_row['units_cd'] = '@'
    if 'end_date' not in dict_row:
        dict_row['end_date'] = None
    dict_row['location_cd'] = '@'
    dict_row['import_date'] = date_import
    dict_row['sourcesystem_cd'] = CODE_SOURCE
    return dict_row


def check_if_encounter_is_imported(connection, table_obs, num_enc):
    """
    Runs a query to check if this script was already used to upload p21 data
    in i2b2crcdata.observation_fact

    Parameters
    ----------
    connection : sqlalchemy.connection
        Connection object of engine to run querys on
    table_obs : sqlalchemy.Table
        Table object of i2b2crcdata.observation_fact
    num_enc : str
        encounter_num of encounter

    Raises
    ------
    SystemExit
        If encounter has multiple sources for given script id

    Returns
    -------
    bool
        True if encounter data is already in database, False otherwise

    """
    query = db.select([table_obs.c['sourcesystem_cd']]) \
        .where(table_obs.c['encounter_num'] == str(num_enc)) \
        .where(table_obs.c['concept_cd'] == 'P21:SCRIPT') \
        .where(table_obs.c['modifier_cd'] == 'scriptId') \
        .where(table_obs.c['tval_char'] == SCRIPT_ID)
    # SELECT observation_fact.sourcesystem_cd FROM observation_fact WHERE observation_fact.encounter_num = %(encounter_num_1)s AND observation_fact.concept_cd = %(concept_cd_1)s AND observation_fact.modifier_cd = %(modifier_cd_1)s AND observation_fact.tval_char = %(tval_char_1)s
    result = connection.execute(query).fetchall()
    if result:
        if len(result) != 1:
            raise SystemExit('multiple sourcesystems for encounter found')
        return True
    return False


def delete_encounter_data(connection, table_obs, num_enc):
    """
    Deletes p21 data of uploaded encounter from i2b2crcdata.observation_fact.
    Can only delete data if it was uploaded using this script (like an older
    version)

    Notes:
        Check is done by matching modifier 'scriptId' of the concept
        'P21:SCRIPT' of given encounter with the id of this script

        If this is the case, the sourcesystem_cd of concept 'P21:SCRIPT'
        is returned

        All entries with given sourecsystem_cd and encounter_num are deleted
        from i2b2crcdata.observation_fact (unique sourcesystem_cd is created
        for uploaded data using this script)

    Parameters
    ----------
    connection : sqlalchemy.connection
        Connection object of engine to run querys on
    table_obs : sqlalchemy.Table
        Table object of i2b2crcdata.observation_fact
    num_enc : str
        encounter_num of encounter

    Raises
    ------
    SystemExit
        If encounter has multiple sources for given script id or if delete
        operation fails

    Returns
    -------
    None.

    """
    query = db.select([table_obs.c['sourcesystem_cd']]) \
        .where(table_obs.c['encounter_num'] == str(num_enc)) \
        .where(table_obs.c['concept_cd'] == 'P21:SCRIPT') \
        .where(table_obs.c['modifier_cd'] == 'scriptId') \
        .where(table_obs.c['tval_char'] == SCRIPT_ID)
    # SELECT observation_fact.sourcesystem_cd FROM observation_fact WHERE observation_fact.encounter_num = %(encounter_num_1)s AND observation_fact.concept_cd = %(concept_cd_1)s AND observation_fact.modifier_cd = %(modifier_cd_1)s AND observation_fact.tval_char = %(tval_char_1)s
    result = connection.execute(query).fetchall()
    if result:
        if len(result) != 1:
            raise SystemExit('multiple sourcesystems for encounter found')
        sourcesystem_cd = result[0][0]
        statement_delete = table_obs.delete() \
            .where(table_obs.c['encounter_num'] == str(num_enc)) \
            .where(table_obs.c['sourcesystem_cd'] == sourcesystem_cd)
        # DELETE FROM observation_fact WHERE observation_fact.encounter_num = %(encounter_num_1)s AND observation_fact.sourcesystem_cd = %(sourcesystem_cd_1)s
        transaction = connection.begin()
        try:
            connection.execute(statement_delete)
            transaction.commit()
        except:
            transaction.rollback()
            traceback.print_exc()
            raise SystemExit('delete operation for encounter failed')
    else:
        global num_cases_new
        num_cases_new = num_cases_new + 1


def upload_encounter_data(connection, table_obs, list_dict):
    """
    Uploads observation_fact rows (as a list of dict) into database table
    i2b2crcdata.observation_fact for given encounter

    Parameters
    ----------
    connection : sqlalchemy.connection
        Connection object of engine to run querys on
    table_obs : sqlalchemy.Table
        Table object of i2b2crcdata.observation_fact
    list_dict : list
        List of observation_fact rows with collected p21 variables of a given
        encounter

    Raises
    ------
    SystemExit
        If upload operation fails

    Returns
    -------
    None.

    """
    transaction = connection.begin()
    try:
        connection.execute(table_obs.insert(), list_dict)
        transaction.commit()
    except:
        transaction.rollback()
        traceback.print_exc()
        raise SystemExit('insert operation for encounter failed')


def print_import_results(num_imports, num_updates):
    """
    Prints import summary about new imported encounter and updated encounter
    in console

    Parameters
    ----------
    num_imports : int
        Number of new imported encounter
    num_updates : int
        Number of updated encounter

    Returns
    -------
    None.

    """
    print('Fälle hochgeladen: %d' % (num_imports + num_updates))
    print('Neue Fälle hochgeladen: %d' % num_imports)
    print('Bestehende Fälle aktualisiert: %d' % num_updates)


"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
MISC
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""


def get_csv_encoding(path_csv):
    """
    Reads the first {CSV_BYTES_CHECK_ENCODER} bytes of a given csv file and
    returns the csv encoding as a str

    Parameters
    ----------
    path_csv : str
        path to the csv-file to check encoding of

    Returns
    -------
    str
        Encoding of given csv-file

    """
    return chardet.detect(open(path_csv, 'rb').read(CSV_BYTES_CHECK_ENCODER))['encoding']


def get_db_engine():
    """
    Extracts connection path (format: HOST:PORT/DB) out of given connection-url
    and creates engine object with given credentials (all via environment
    variables) to enable a database connection

    Returns
    -------
    sqlalchemy.engine
        Engine object which enables a connection with i2b2crcdata

    """
    pattern = 'jdbc:postgresql://(.*?)(\?searchPath=.*)?$'
    connection = re.search(pattern, I2B2_CONNECTION_URL).group(1)
    return db.create_engine('postgresql+psycopg2://{0}:{1}@{2}'.format(USERNAME, PASSWORD, connection))


def get_aktin_property(property_aktin):
    """
    Searches aktin.properties for given key and returns the corresponding value

    Parameters
    ----------
    property_aktin : str
        Key of the requested property

    Returns
    -------
    str
        Corresponding value of requested key or empty string if not found

    """
    if not os.path.exists(PATH_AKTIN_PROPERTIES):
        raise SystemExit('file path for aktin.properties is not valid')
    with open(PATH_AKTIN_PROPERTIES) as properties:
        for line in properties:
            if '=' in line:
                key, value = line.split('=', 1)
                if (key == property_aktin):
                    return value.strip()
        return ''


def anonymize_enc_list(list_enc):
    """
    Gets the root.preset from aktin.properties as well as stated cryptographic
    hash function and cryptographic salt and uses them to hash encounter ids
    of a given list

    Parameters
    ----------
    list_enc : list
        List with encounter ids

    Returns
    -------
    list_enc_ide : list
        List with hashed encounter ids

    """
    root = get_aktin_property('cda.encounter.root.preset')
    salt = get_aktin_property('pseudonym.salt')
    alg = get_aktin_property('pseudonym.algorithm')
    return [one_way_anonymizer(alg, root, enc, salt) for enc in list_enc]


def one_way_anonymizer(name_alg, root, extension, salt):
    """
    Hashes given encounter id with given algorithm, root.preset and salt. If
    no algorithm was stated, sha1 is used

    Parameters
    ----------
    name_alg : str
        Name of cryptographic hash function from aktin.properties
    root : str
        Root preset from aktin.properties
    extension : str
        Encounter id to hash
    salt : str
        Cryptographic salt from aktin.properties

    Returns
    -------
    str
        Hashed encounter id

    """
    name_alg = convert_crypto_alg_name(name_alg) if name_alg else 'sha1'
    composite = '/'.join([str(root), str(extension)])
    composite = salt + composite if salt else composite
    buffer = composite.encode('UTF-8')
    alg = getattr(hashlib, name_alg)()
    alg.update(buffer)
    return base64.urlsafe_b64encode(alg.digest()).decode('UTF-8')


def convert_crypto_alg_name(name_alg):
    """
    Converts given name of java cryptograhpic hash function to python demanted
    format, example:
        MD5 -> md5
        SHA-1 -> sha1
        SHA-512/224 -> sha512_224

    Parameters
    ----------
    name_alg : str
        Name to convert to python format

    Returns
    -------
    str
        Converted name of hash function

    """
    return str.lower(name_alg.replace('-', '', ).replace('/', '_'))


def convert_date_to_i2b2_format(date):
    """
    Converts a string date from %Y%m%d%H%M to %Y-%m-%d %H:%M. Used to convert
    p21 dates from zip-file to i2b2crcdata.observation_fact format

    Parameters
    ----------
    num_date : str
        Date in format %Y%m%d%H%M

    Returns
    -------
    str
        Date in format %Y-%m-%d %H:%M

    """
    return datetime.strptime(str(date), '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M')


def convert_icd_code_to_i2b2_format(code_icd):
    """
    Converts icd code to i2b2crcdata.observation_fact format by checking and
    adding (if necessary) '.'-delimiter at index 3. Does not add delimiter if
    icd code is only 3 characters long

    Example:
        F2424 -> F24.24
        F24.24 -> F24.24
        J90 -> J90
        J21. -> J21.

    Parameters
    ----------
    code_icd : str
        icd code to convert

    Returns
    -------
    str
        icd code with added delimiter

    """
    if len(code_icd) > 3:
        code_icd = ''.join([code_icd[:3], '.', code_icd[3:]] if code_icd[3] != '.' else code_icd)
    return code_icd


def convert_ops_code_to_i2b2_format(code_ops):
    """
    Converts ops code to i2b2crcdata.observation_fact format by checking and
    adding (if necessary) '-'-delimiter at index 1 and '.'-delimiter at index 5.
    Second delimiter is only added, if more than three digits follow first
    delimiter

    Example:
        964922 -> 9-649.22
        9-64922 -> 9-649.22
        9649.22 -> 9-649.22
        9-649.22 -> 9-649.22
        1-5020 -> 1-502.0
        1-501 -> 1-501
        1051 -> 1-501

    Parameters
    ----------
    code_ops : str
        ops code to convert

    Returns
    -------
    code_ops : str
        ops code with added delimiter

    """
    code_ops = ''.join([code_ops[:1], '-', code_ops[1:]] if code_ops[1] != '-' else code_ops)
    if len(code_ops) > 5:
        code_ops = ''.join([code_ops[:5], '.', code_ops[5:]] if code_ops[5] != '.' else code_ops)
    return code_ops


"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
MAIN
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""

if __name__ == '__main__':
    # required file names in zip-file and required columns for each file
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

    # columns which must not contain an empty field
    LIST_P21_COLUMN_MANDATORY = [
        'kh-internes-kennzeichen',
        'aufnahmedatum',
        'aufnahmegrund',
        'aufnahmeanlass',
        'fachabteilung',
        'fab-aufnahmedatum',
        'kennung-intensivbett',
        'diagnoseart',
        'icd-version',
        'icd-kode',
        'ops-version',
        'ops-kode',
        'ops-datum'
    ]

    # format requirements for each column
    DICT_P21_COLUMN_PATTERN = {
        'kh-internes-kennzeichen': '^.*$',
        'ik-der-krankenkasse': '^\w*$',
        'geburtsjahr': '^(19|20)\d{2}$',
        'geschlecht': '^[mwdx]$',
        'plz': '^\d{5}$',
        'aufnahmedatum': '^\d{12}$',
        'aufnahmegrund': '^(0[1-9]|10)\d{2}$',
        'aufnahmeanlass': '^[EZNRVAGB]$',
        'fallzusammenführung': '^(J|N)$',
        'fallzusammenführungsgrund': '^OG|MD|KO|RU|WR|MF|P[WRM]|Z[OMKRW]$',
        'verweildauer-intensiv': '^\d*(,\d{2})?$',
        'entlassungsdatum': '^\d{12}$',
        'entlassungsgrund': '^\d{2}.{1}$',
        'beatmungsstunden': '^\d*(,\d{2})?$',
        'behandlungsbeginn-vorstationär': '^\d{8}$',
        'behandlungstage-vorstationär': '^\d$',
        'behandlungsende-nachstationär': '^\d{8}$',
        'behandlungstage-nachstationär': '^\d$',
        'fachabteilung': '^(HA|BA|BE)\d{4}$',
        'fab-aufnahmedatum': '^\d{12}$',
        'fab-entlassungsdatum': '^\d{12}$',
        'kennung-intensivbett': '^(J|N)$',
        'diagnoseart': '^(HD|ND|SD)$',
        'icd-version': '^20\d{2}$',
        'icd-kode': '^[A-Z]\d{2}(\.)?.{0,3}$',
        'lokalisation': '^[BLR]$',
        'diagnosensicherheit': '^[AVZG]$',
        'sekundär-kode': '^[A-Z]\d{2}(\.)?.{0,3}$',
        'sekundär-lokalisation': '^[BLR]$',
        'sekundär-diagnosensicherheit': '^[AVZG]$',
        'ops-version': '^20\d{2}$',
        'ops-kode': '^\d{1}(\-)?\d{2}(.{1})?(\.)?.{0,2}$',
        'ops-datum': '^\d{12}$'
    }

    CSV_SEPARATOR = ';'
    CSV_BYTES_CHECK_ENCODER = 1024
    CSV_CHUNKSIZE = 10000
    DB_CHUNKSIZE = 10000

    USERNAME = 'i2b2crcdata'
    PASSWORD = 'demouser'
    I2B2_CONNECTION_URL = 'jdbc:postgresql://localhost:5432/i2b2?searchPath=i2b2crcdata'
    ZIP_UUID = '3fc5b451-3333-1245-1134-a70bfc58fd1f'
    SCRIPT_ID = 'p21import'
    SCRIPT_VERSION = '1.2'
    PATH_AKTIN_PROPERTIES = r'C:\Users\User\IdeaProjects\dwh-setup\dwh-update\src\main\scripts\aktin.properties'
    CODE_SOURCE = '_'.join(['i', SCRIPT_ID, ZIP_UUID])

    PATH_P21 = r'C:\Users\User\PycharmProjects\p21-script\test\resources\p21_verification.zip'

    import time

    start_time = time.time()
    import_file(PATH_P21)
    print("--- %s seconds ---" % (time.time() - start_time))

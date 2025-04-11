# -*- coding: utf-8 -*
# Created on Wed Jan 20 11:36:55 2021
# @VERSION=1.6
# @VIEWNAME=P21-Importskript
# @MIMETYPE=zip
# @ID=p21

#
#      Copyright (c) 2025  AKTIN
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU Affero General Public License as
#      published by the Free Software Foundation, either version 3 of the
#      License, or (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU Affero General Public License for more details.
#
#      You should have received a copy of the GNU Affero General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#

import base64
import hashlib
import os
import re
import shutil
import sys
import traceback
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
import sqlalchemy as db
from sqlalchemy import exc

"""
Script to verify and import p21 data into the AKTIN DWH:
- checks validity of csv files in given zip-file regarding p21 requirements
- matches valid encounter in fall.csv with encounters in database
- matching is done by the billing_id, and the encounter_id as a fallback
- iterates through matched encounters in fall.csv
- all valid fields of valid encounters are uploaded into the i2b2 database as observation facts
- already uploaded encounter by this script are deleted prior upload
- after uploading each valid encounter from fall.csv, the script iterates 
through the optional csv-files (fab,icd,ops) and uploads their facts too
"""

class P21Importer:

  def __init__(self, path_zip: str):
    self.__zfe = ZipFileExtractor(path_zip)
    path_parent = os.path.dirname(path_zip)
    self.__tfm = TmpFolderManager(path_parent)
    self.__num_imports = 0
    self.__num_updates = 0

  def __extract_and_rename_zip_content(self) -> str:
    path_tmp = self.__tfm.create_tmp_folder()
    self.__zfe.extract_zip_to_folder(path_tmp)
    self.__tfm.rename_files_in_tmp_folder_to_lowercase()
    return path_tmp

  def __preprocess_and_check_csv_files(self, path_folder: str):
    for v, p in [
      (FALLVerifier, FALLPreprocessor),
      (FABVerifier, FABPreprocessor),
      (ICDVerifier, ICDPreprocessor),
      (OPSVerifier, OPSPreprocessor),
    ]:
      verifier = v(path_folder)
      preprocessor = p(path_folder)
      if verifier.is_csv_in_folder():
        preprocessor.preprocess()
        verifier.check_column_names_of_csv()

  def __get_matched_encounters(self, list_valid_ids: list) -> pd.DataFrame:
    try:
      extractor = EncounterInfoExtractorWithBillingId()
      matcher = DatabaseEncounterMatcher(extractor)
      return matcher.get_matched_df(list_valid_ids)
    except ValueError:
      print("Matching by billing id failed. trying matching by encounter id...")
      extractor = EncounterInfoExtractorWithEncounterId()
      matcher = DatabaseEncounterMatcher(extractor)
      return matcher.get_matched_df(list_valid_ids)

  def __enrich_with_admission_dates(self, verifier_fall, df_mapping: pd.DataFrame) -> pd.DataFrame:
    dict_admission_dates = verifier_fall.get_unique_ids_of_valid_encounter_with_admission_dates()
    df_admission_dates = pd.DataFrame({
      "encounter_id": list(dict_admission_dates.keys()),
      "aufnahmedatum": list(dict_admission_dates.values()),
    })
    return pd.merge(df_mapping, df_admission_dates, on=["encounter_id"])

  def __print_verification_stats(self, verifier_fall, list_valid_ids: list, df_mapping: pd.DataFrame):
    print(f"Fälle gesamt: {verifier_fall.count_total_encounter()}")
    print(f"Fälle valide: {len(list_valid_ids)}")
    print(f"Valide Fälle gematcht mit Datenbank: {df_mapping.shape[0]}")

  def __import_observation_facts(self, df_mapping: pd.DataFrame, path_tmp:str):
    for uploader_class in [
      FALLObservationFactUploadManager,
      FABObservationFactUploadManager,
      ICDObservationFactUploadManager,
      OPSObservationFactUploadManager,
    ]:
      uploader = uploader_class(df_mapping, path_tmp)
      if uploader.VERIFIER.is_csv_in_folder():
        uploader.upload_csv()
      # Store metrics for unique encounters
      if isinstance(uploader, FALLObservationFactUploadManager):
        self.__num_imports = uploader.NUM_IMPORTS
        self.__num_updates = uploader.NUM_UPDATES

  def __print_import_results(self):
    print(f"Fälle hochgeladen: {self.__num_imports + self.__num_updates}")
    print(f"Neue Fälle hochgeladen: {self.__num_imports}")
    print(f"Bestehende Fälle aktualisiert: {self.__num_updates}")

  def import_file(self):
    """Handles the full file import and data upload process.

      Performs:
      1. Data validation and temporary file preparation
      2. Database matching of valid encounters
      3. CSV uploads for different observation types (FALL, FAB, ICD, OPS)
      4. Final cleanup of temporary resources

      Prints:
          Validation statistics and upload results summary
      Raises:
          Exception: Propagates any errors from processing steps (final cleanup always occurs)
      """
    try:
      path_tmp = self.__extract_and_rename_zip_content()
      self.__preprocess_and_check_csv_files(path_tmp)
      verifier_fall = FALLVerifier(path_tmp)
      list_valid_ids = verifier_fall.get_unique_ids_of_valid_encounter()
      df_mapping = self.__get_matched_encounters(list_valid_ids)
      df_mapping = self.__enrich_with_admission_dates(verifier_fall, df_mapping)
      self.__print_verification_stats(verifier_fall, list_valid_ids, df_mapping)
      self.__import_observation_facts(df_mapping, path_tmp)
      self.__print_import_results()
    finally:
      self.__tfm.remove_tmp_folder()


class ZipFileExtractor:

    def __init__(self, path_zip: str):
        self.PATH_ZIP = path_zip
        self.__check_zip_file_integrity()

    def __check_zip_file_integrity(self):
        if not os.path.exists(self.PATH_ZIP):
            raise SystemExit('file path is not valid')
        if not zipfile.is_zipfile(self.PATH_ZIP):
            raise SystemExit('file is not a zipfile')

    def extract_zip_to_folder(self, path_folder: str):
        with zipfile.ZipFile(self.PATH_ZIP, 'r') as file_zip:
            file_zip.extractall(path_folder)


class TmpFolderManager:
    """
    Creates a temporary folder named tmp where the csv files inside the zip
    file can be extracted to for preprocessing.
    Renames all files inside folder tmp to lowercase for case-insensitive
    processing.
    """

    def __init__(self, path_folder: str):
        self.PATH_TMP = os.path.join(path_folder, 'tmp')

    def create_tmp_folder(self) -> str:
        if not os.path.isdir(self.PATH_TMP):
            os.makedirs(self.PATH_TMP)
        return self.PATH_TMP

    def remove_tmp_folder(self):
        if os.path.isdir(self.PATH_TMP):
            shutil.rmtree(self.PATH_TMP)

    def rename_files_in_tmp_folder_to_lowercase(self):
        list_files = self.__get_files_in_tmp_folder()
        for file in list_files:
            path_file = os.path.join(self.PATH_TMP, file)
            path_file_lower = os.path.join(self.PATH_TMP, file.lower())
            os.rename(path_file, path_file_lower)

    def __get_files_in_tmp_folder(self) -> list:
        return [file for file in os.listdir(self.PATH_TMP) if os.path.isfile(os.path.join(self.PATH_TMP, file))]


class CSVReader(ABC):
    """
    Provides configuration for reading a csv file of given path.
    """

    SIZE_CHUNKS: int = 10000
    CSV_SEPARATOR: str = ';'
    CSV_NAME: str

    def __init__(self, path_folder: str):
        self.PATH_CSV = os.path.join(path_folder, self.CSV_NAME)

    @staticmethod
    def get_csv_encoding() -> str:
        return 'utf-8'

    def save_df_as_csv(self, df_input: pd.DataFrame, path_output: str, encoding: str):
        df_input.to_csv(path_output, sep=self.CSV_SEPARATOR, encoding=encoding, index=False)


class CSVPreprocessor(CSVReader, ABC):
    """
    Preprocesses a given csv file to adjust it to the requirements for
    CSVFileVerifier/CSVObservationFactConverter/CSVObservationFactUploadManager.
    """
    LEADING_ZEROS = 0

    def preprocess(self):
        header = self._get_csv_file_header_in_lowercase()
        header = self._remove_dashes_from_header(header)
        header += '\n'
        self._write_header_to_csv(header)
        self._append_zeros_to_internal_id()

    def _get_csv_file_header_in_lowercase(self) -> str:
        df = pd.read_csv(self.PATH_CSV, nrows=0, index_col=None, sep=self.CSV_SEPARATOR, encoding=self.get_csv_encoding(), dtype=str)
        df.rename(columns=str.lower, inplace=True)
        return ';'.join(df.columns)

    @staticmethod
    def _remove_dashes_from_header(header: str) -> str:
        return header.replace('-', '')

    def _write_header_to_csv(self, header: str):
        path_parent = os.path.dirname(self.PATH_CSV)
        path_dummy = os.path.sep.join([path_parent, 'dummy.csv'])
        encoding = self.get_csv_encoding()
        with open(self.PATH_CSV, 'r+', encoding=encoding) as f1, open(path_dummy, 'w+', encoding=encoding) as f2:
            f1.readline()
            f2.write(header)
            shutil.copyfileobj(f1, f2)
        os.remove(self.PATH_CSV)
        os.rename(path_dummy, self.PATH_CSV)

    def _rename_column_in_header(self, header: str, column_old: str, column_new: str) -> str:
        list_header = header.split(self.CSV_SEPARATOR)
        if list_header.count(column_new) == 1:
            return header
        pattern = ''.join([r'^', column_old, r'(\.)?(\d*)?$'])
        idx_match = [i for i, item in enumerate(list_header) if re.search(pattern, item)]
        if len(idx_match) != 1:
            raise SystemExit('invalid count for column of %s during adjustment' % column_old)
        list_header[idx_match[0]] = column_new
        return self.CSV_SEPARATOR.join(list_header)

    def _append_zeros_to_internal_id(self):
        path_parent = os.path.dirname(self.PATH_CSV)
        path_dummy = os.path.sep.join([path_parent, 'dummy.csv'])
        encoding = self.get_csv_encoding()
        df_tmp = pd.DataFrame()
        for chunk in pd.read_csv(self.PATH_CSV, chunksize=self.SIZE_CHUNKS, sep=self.CSV_SEPARATOR, encoding=encoding, dtype=str):
            chunk['khinterneskennzeichen'] = chunk['khinterneskennzeichen'].fillna('')
            chunk['khinterneskennzeichen'] = chunk['khinterneskennzeichen'].apply(lambda x: ''.join([str('0' * self.LEADING_ZEROS), x]))
            df_tmp = pd.concat([df_tmp, chunk])
        self.save_df_as_csv(df_tmp, path_dummy, encoding)
        os.remove(self.PATH_CSV)
        os.rename(path_dummy, self.PATH_CSV)


class FALLPreprocessor(CSVPreprocessor):
    CSV_NAME = 'fall.csv'

    def preprocess(self):
        super().preprocess()
        self.__append_zero_to_column_if_length_below_requirement('plz', 5)
        self.__append_zero_to_column_if_length_below_requirement('aufnahmegrund', 4)

    def __append_zero_to_column_if_length_below_requirement(self, column: str, length_required: int):
        path_parent = os.path.dirname(self.PATH_CSV)
        path_dummy = os.path.sep.join([path_parent, 'dummy.csv'])
        encoding = self.get_csv_encoding()
        df_tmp = pd.DataFrame()
        for chunk in pd.read_csv(self.PATH_CSV, chunksize=self.SIZE_CHUNKS, sep=self.CSV_SEPARATOR, encoding=encoding, dtype=str):
            chunk[column] = chunk[column].fillna('')
            chunk[column] = chunk[column].apply(lambda x: x.rjust(length_required, '0') if len(x) == length_required - 1 else x)
            df_tmp = pd.concat([df_tmp, chunk])
        self.save_df_as_csv(df_tmp, path_dummy, encoding)
        os.remove(self.PATH_CSV)
        os.rename(path_dummy, self.PATH_CSV)


class FABPreprocessor(CSVPreprocessor):
    CSV_NAME = 'fab.csv'

    def preprocess(self):
        header = self._get_csv_file_header_in_lowercase()
        header = self._remove_dashes_from_header(header)
        header = self._rename_column_in_header(header, 'fab', 'fachabteilung')
        header += '\n'
        self._write_header_to_csv(header)
        self._append_zeros_to_internal_id()


class ICDPreprocessor(CSVPreprocessor):
    """
    Overrides preprocess() to additionally add columns for secondary diagnoses
    if missing
    """

    CSV_NAME = 'icd.csv'

    def preprocess(self):
        header = self._get_csv_file_header_in_lowercase()
        header = self._remove_dashes_from_header(header)
        if 'sekundärkode' in header:
            header = self.__adjust_columns_for_secondary_diagnoses(header)
            header += '\n'
            self._write_header_to_csv(header)
        else:
            self.__write_header_with_secondary_diagnoses_columns_to_csv(header)
        self._append_zeros_to_internal_id()

    def __adjust_columns_for_secondary_diagnoses(self, header: str) -> str:
        index_sec = header.index('sekundärkode')
        header_sub = header[index_sec:]
        header_sub = self._rename_column_in_header(header_sub, 'lokalisation', 'sekundärlokalisation')
        header_sub = self._rename_column_in_header(header_sub, 'diagnosensicherheit', 'sekundärdiagnosensicherheit')
        return ''.join([header[:index_sec], header_sub])

    def __write_header_with_secondary_diagnoses_columns_to_csv(self, header: str):
        list_header = header.split(self.CSV_SEPARATOR)
        df = pd.read_csv(self.PATH_CSV, sep=self.CSV_SEPARATOR, encoding=self.get_csv_encoding(), dtype=str)
        df.columns = list_header
        df['sekundärkode'] = ''
        df['sekundärlokalisation'] = ''
        df['sekundärdiagnosensicherheit'] = ''
        df.to_csv(self.PATH_CSV, index=False, sep=self.CSV_SEPARATOR)


class OPSPreprocessor(CSVPreprocessor):
    CSV_NAME = 'ops.csv'


class CSVFileVerifier(CSVReader, ABC):
    """
    Verifies a csv file by checking its existence and existence of required column
    names. Existence of a csv file is optional, but the required columns must be in
    the csv file if it exists.

    DICT_COLUMN_PATTERN -> { name of required csv column : regex pattern for column value }

    MANDATORY_COLUMN_VALUES -> list of column names where the value must not be empty
    or invalid

    DICT_COLUMN_PATTERN and MANDATORY_COLUMN_VALUES must be set in implementing
    classes
    """

    DICT_COLUMN_PATTERN: dict
    MANDATORY_COLUMN_VALUES: list

    def is_csv_in_folder(self) -> bool:
        if not os.path.isfile(self.PATH_CSV):
            print('{0} could not be found in zip'.format(self.CSV_NAME))
            return False
        return True

    def check_column_names_of_csv(self):
        df = pd.read_csv(self.PATH_CSV, nrows=0, index_col=None, sep=self.CSV_SEPARATOR, encoding=self.get_csv_encoding(), dtype=str)
        set_required_columns = set(self.DICT_COLUMN_PATTERN.keys())
        set_matched_columns = set_required_columns.intersection(set(df.columns))
        if set_matched_columns != set_required_columns:
            raise SystemExit('following columns are missing in {0}: {1}'.format(self.CSV_NAME, set_required_columns.difference(set_matched_columns)))

    def get_unique_ids_of_valid_encounter(self) -> list:
        set_valid_ids = set()
        for chunk in pd.read_csv(self.PATH_CSV, chunksize=self.SIZE_CHUNKS, sep=self.CSV_SEPARATOR, encoding=self.get_csv_encoding(), dtype=str):
            chunk = chunk[list(self.DICT_COLUMN_PATTERN.keys())]
            chunk = chunk.fillna('')
            for column in chunk.columns.values:
                chunk = self.clear_invalid_column_fields_in_chunk(chunk, column)
            set_valid_ids.update(chunk['khinterneskennzeichen'].unique())
        return list(set_valid_ids)

    def clear_invalid_column_fields_in_chunk(self, chunk: pd.Series, column_name: str) -> pd.Series:
        """
        Filters a chunk of a csv dataframe by a given column. All values in column are
        checked by the pattern set for this column in DICT_COLUMN_PATTERN.

        If column name appears in MANDATORY_COLUMN_VALUES, the whole row of the chunk
        is dropped if the column value is empty or does not match the pattern.

        If column name does not appear in MANDATORY_COLUMN_VALUES, column values which
        do not match the pattern are cleared/emptyed (meaning it will not be imported)
        """
        pattern = self.DICT_COLUMN_PATTERN[column_name]
        indeces_empty_fields = chunk[chunk[column_name] == ''].index
        indeces_wrong_syntax = chunk[(chunk[column_name] != '') & (~chunk[column_name].str.match(pattern))].index
        if len(indeces_wrong_syntax):
            if column_name not in self.MANDATORY_COLUMN_VALUES:
                chunk.loc[indeces_wrong_syntax, column_name] = ''
            else:
                chunk = chunk.drop(indeces_wrong_syntax)
        if len(indeces_empty_fields) and column_name in self.MANDATORY_COLUMN_VALUES:
            chunk = chunk.drop(indeces_empty_fields)
        return chunk


class FALLVerifier(CSVFileVerifier):
    """
    fall.csv is a mandatory csv file. Overrides is_csv_in_folder() and get_unique_ids_of_valid_encounter()
    to exit script if fall.csv does not exist or does not contain any valid encounter
    """

    CSV_NAME = 'fall.csv'
    DICT_COLUMN_PATTERN = {'khinterneskennzeichen':         r'^.*$',
                           'ikderkrankenkasse':             r'^\w*$',
                           'geburtsjahr':                   r'^(19|20)\d{2}$',
                           'geschlecht':                    r'^[mwdx]$',
                           'plz':                           r'^\d{5}$',
                           'aufnahmedatum':                 r'^\d{12}$',
                           'aufnahmegrund':                 r'^(0[1-9]|10)\d{2}$',
                           'aufnahmeanlass':                r'^[EZNRVAGB]$',
                           'fallzusammenführung':           r'^(J|N)$',
                           'fallzusammenführungsgrund':     r'^OG|MD|KO|RU|WR|MF|P[WRM]|Z[OMKRW]$',
                           'verweildauerintensiv':          r'^\d*(,\d{2})?$',
                           'entlassungsdatum':              r'^\d{12}$',
                           'entlassungsgrund':              r'^\d{2}.{1}$',
                           'beatmungsstunden':              r'^\d*(,\d{2})?$',
                           'behandlungsbeginnvorstationär': r'^\d{8}$',
                           'behandlungstagevorstationär':   r'^\d{0,4}$',
                           'behandlungsendenachstationär':  r'^\d{8}$',
                           'behandlungstagenachstationär':  r'^\d{0,4}$'}
    MANDATORY_COLUMN_VALUES = ['khinterneskennzeichen', 'aufnahmedatum', 'aufnahmegrund', 'aufnahmeanlass']

    def is_csv_in_folder(self) -> bool:
        if not os.path.isfile(self.PATH_CSV):
            raise SystemExit('fall.csv is a mandatory file and could not be found in zip')
        return True

    def get_unique_ids_of_valid_encounter(self) -> list:
        list_valid_ids = super().get_unique_ids_of_valid_encounter()
        if not list_valid_ids:
            raise SystemExit('no valid encounter found in fall.csv')
        return list_valid_ids

    def count_total_encounter(self) -> int:
        with open(self.PATH_CSV, encoding=self.get_csv_encoding()) as csv:
            total = sum(1 for _ in csv)
        return total - 1

    def get_unique_ids_of_valid_encounter_with_admission_dates(self) -> dict:
        """
        Same as get_unique_valid_encouter_ids(), but returns a dict with { encounter_id : admission_date }
        This dict is used together with the output of DatabaseEncounterMatcher.get_matched_df()
        to create the mapping dataframe required by CSVObservationFactUploadManager
        """
        dict_case_admissions = {}
        for chunk in pd.read_csv(self.PATH_CSV, chunksize=self.SIZE_CHUNKS, sep=self.CSV_SEPARATOR, encoding=self.get_csv_encoding(), dtype=str):
            chunk = chunk[list(self.DICT_COLUMN_PATTERN.keys())]
            chunk = chunk.fillna('')
            for column in chunk.columns.values:
                chunk = self.clear_invalid_column_fields_in_chunk(chunk, column)
            dict_chunk = dict(zip(chunk['khinterneskennzeichen'], chunk['aufnahmedatum']))
            dict_case_admissions = {**dict_case_admissions, **dict_chunk}
        if not dict_case_admissions:
            raise SystemExit('no valid encounter found in fall.csv')
        return dict_case_admissions


class FABVerifier(CSVFileVerifier):
    CSV_NAME = 'fab.csv'
    DICT_COLUMN_PATTERN = {'khinterneskennzeichen': r'^.*$',
                           'fachabteilung':         r'^(HA|BA|BE)\d{4}$',
                           'fabaufnahmedatum':      r'^\d{12}$',
                           'fabentlassungsdatum':   r'^\d{12}$',
                           'kennungintensivbett':   r'^(J|N)$'}
    MANDATORY_COLUMN_VALUES = ['khinterneskennzeichen', 'fachabteilung', 'fabaufnahmedatum', 'kennungintensivbett']


class ICDVerifier(CSVFileVerifier):
    CSV_NAME = 'icd.csv'
    DICT_COLUMN_PATTERN = {'khinterneskennzeichen':       r'^.*$',
                           'diagnoseart':                 r'^(HD|ND|SD)$',
                           'icdversion':                  r'^20\d{2}$',
                           'icdkode':                     r'^[A-Z]\d{2}(\.)?.{0,5}$',
                           'lokalisation':                r'^[BLR]$',
                           'diagnosensicherheit':         r'^[AVZG]$',
                           'sekundärkode':                r'^[A-Z]\d{2}(\.)?.{0,5}$',
                           'sekundärlokalisation':        r'^[BLR]$',
                           'sekundärdiagnosensicherheit': r'^[AVZG]$'}
    MANDATORY_COLUMN_VALUES = ['khinterneskennzeichen', 'diagnoseart', 'icdversion', 'icdkode']


class OPSVerifier(CSVFileVerifier):
    CSV_NAME = 'ops.csv'
    DICT_COLUMN_PATTERN = {'khinterneskennzeichen': r'^.*$',
                           'opsversion':            r'^20\d{2}$',
                           'opskode':               r'^\d{1}(\-)?\d{2}(.{1})?(\.)?.{0,3}$',
                           'opsdatum':              r'^\d{12}$',
                           'lokalisation':          r'^[BLR]$'}
    MANDATORY_COLUMN_VALUES = ['khinterneskennzeichen', 'opsversion', 'opskode', 'opsdatum']


class CSVObservationFactConverter(ABC):
    """
    Converts a row from a given csv file to a list of observation fact dictionaries to
    upload to the table i2b2crcdata.observation_fact in the database.

    Each dictionary corresponds to one row in the database. A list of dicts is created
    as a row in the csv file may need multiple rows in the database.

    Only mandatory database row values shall be added in create_observation_facts_from_row().
    Default values (like provider_id or sourcesystem_cd) shall be added through
    add_static_values_to_row_dict().
    """

    def __init__(self):
        self.SCRIPT_ID = os.environ['script_id']
        self.ZIP_UUID = os.environ['uuid']
        self.CODE_SOURCE = '_'.join([self.SCRIPT_ID, self.ZIP_UUID])

    @abstractmethod
    def create_observation_facts_from_row(self, row_csv: pd.Series) -> list:
        pass

    def add_static_values_to_row_dict(self, dict_row: dict, num_enc: str, num_pat: str, date_admission: str) -> dict:
        date_import = datetime.now(tz=None).strftime('%Y-%m-%d %H:%M:%S.%f')
        date_admission = self._convert_date_to_i2b2_format(date_admission)
        dict_row['encounter_num'] = num_enc
        dict_row['patient_num'] = num_pat
        dict_row['provider_id'] = 'P21'
        if 'start_date' not in dict_row:
            dict_row['start_date'] = date_admission
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
        dict_row['update_date'] = date_import
        dict_row['download_date'] = date_import
        dict_row['sourcesystem_cd'] = self.CODE_SOURCE
        return dict_row

    @staticmethod
    def _convert_date_to_i2b2_format(date: str) -> str:
        if date[8:10] == '24':
            date = ''.join([date[:8], '2359'])
        return datetime.strptime(str(date), '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M')


class FALLObservationFactConverter(CSVObservationFactConverter):
    """
    In fall.csv, only the columns 'aufnahmedatum','aufnahmegrund' and 'aufnahmeanlass'
    are mandatory Other columns may be empty and are only added, if the columns contains
    a value.

    Columns 'entlassungsdatum' and 'entlassungsgrund' are only added, if both columns
    contain a value.

    Columns 'fallzusammenführung' and 'fallzusammenführungsgrund' are only added, if
    both columns contain a value and 'fallzusammenführung' equals 'J'.

    Column 'behandlungstagevorstationär' is only added, if 'behandlungsbeginnvorstationär'
    contains a value, but is not mandatory for 'behandlungsbeginnvorstationär' to be added.
    Same goes for 'behandlungstagenachstationär' with 'behandlungsendenachstationär'.

    Additionally, contains a method to add metdata of this script as observation facts.
    """

    def __init__(self):
        super().__init__()
        self.SCRIPT_VERSION = os.environ['script_version']

    def create_observation_facts_from_row(self, row_csv: pd.Series) -> list:
        facts = []
        facts.extend(self.__create_admission_dicts(row_csv['aufnahmeanlass'], row_csv['aufnahmegrund']))
        if row_csv['ikderkrankenkasse']:
            facts.append(self.__create_insurance_dict(row_csv['ikderkrankenkasse']))
        if row_csv['geburtsjahr']:
            facts.extend(self.__create_birthyear_dicts(row_csv['geburtsjahr'], row_csv['aufnahmedatum']))
        if row_csv['geschlecht']:
            facts.append(self.__create_sex_dict(row_csv['geschlecht']))
        if row_csv['plz']:
            facts.append(self.__create_zipcode_dict(row_csv['plz']))
        if row_csv['fallzusammenführung'] == 'J' and row_csv['fallzusammenführungsgrund']:
            facts.append(self.__create_encounter_merge_dict(row_csv['fallzusammenführungsgrund']))
        if row_csv['verweildauerintensiv']:
            facts.append(self.__create_critical_care_dict(row_csv['verweildauerintensiv']))
        if row_csv['entlassungsdatum'] and row_csv['entlassungsgrund']:
            facts.append(self.__create_discharge_dict(row_csv['entlassungsdatum'], row_csv['entlassungsgrund']))
        if row_csv['beatmungsstunden']:
            facts.append(self.__create_ventilation_dict(row_csv['beatmungsstunden']))
        if row_csv['behandlungsbeginnvorstationär']:
            facts.append(self.__create_prestation_therapy_start_dict(row_csv['behandlungsbeginnvorstationär'], row_csv['behandlungstagevorstationär']))
        if row_csv['behandlungsendenachstationär']:
            facts.append(self.__create_poststation_therapy_end_dict(row_csv['behandlungsendenachstationär'], row_csv['behandlungstagenachstationär']))
        return facts

    @staticmethod
    def __create_admission_dicts(cause: str, reason: str) -> list:
        concept_cause = ':'.join(['P21:ADMC', str.upper(cause)])
        concept_reason = ':'.join(['P21:ADMR', str.upper(reason)])
        return [{'concept_cd': concept_cause, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'},
                {'concept_cd': concept_reason, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}]

    @staticmethod
    def __create_insurance_dict(insurance: str) -> dict:
        return {'concept_cd': 'AKTIN:IKNR', 'modifier_cd': '@', 'valtype_cd': 'T', 'tval_char': insurance}

    @staticmethod
    def __create_birthyear_dicts(birthyear: str, date_admission: str) -> list:
        return [{'concept_cd': 'LOINC:80904-6', 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': birthyear, 'units_cd': 'yyyy'},
                {'concept_cd': 'LOINC:80904-6', 'modifier_cd': 'effectiveTime', 'valtype_cd': 'T', 'tval_char': date_admission}]

    @staticmethod
    def __create_sex_dict(sex: str) -> dict:
        concept = ':'.join(['P21:SEX', str.upper(sex)])
        return {'concept_cd': concept, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}

    @staticmethod
    def __create_zipcode_dict(zipcode: str) -> dict:
        return {'concept_cd': 'AKTIN:ZIPCODE', 'modifier_cd': '@', 'valtype_cd': 'T', 'tval_char': zipcode}

    @staticmethod
    def __create_encounter_merge_dict(reason: str) -> dict:
        concept = ':'.join(['P21:MERGE', str.upper(reason)])
        return {'concept_cd': concept, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}

    @staticmethod
    def __create_critical_care_dict(intensive: str) -> dict:
        intensive = intensive.replace(',', '.')
        return {'concept_cd': 'P21:DCC', 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': intensive, 'units_cd': 'd'}

    @staticmethod
    def __create_discharge_dict(date: str, reason: str):
        date = CSVObservationFactConverter._convert_date_to_i2b2_format(date)
        concept = ':'.join(['P21:DISR', str.upper(reason)])
        return {'concept_cd': concept, 'start_date': date, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}

    @staticmethod
    def __create_ventilation_dict(ventilation: str) -> dict:
        ventilation = ventilation.replace(',', '.')
        return {'concept_cd': 'P21:DV', 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': ventilation, 'units_cd': 'h'}

    @staticmethod
    def __create_prestation_therapy_start_dict(date_start: str, days: str) -> dict:
        """
        Date information in csv is missing hours and minutes, so dummy values are added
        """
        date_start = CSVObservationFactConverter._convert_date_to_i2b2_format(''.join([date_start, '0000']))
        if days:
            return {'concept_cd': 'P21:PREADM', 'start_date': date_start, 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': days, 'units_cd': 'd'}
        else:
            return {'concept_cd': 'P21:PREADM', 'start_date': date_start, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}

    @staticmethod
    def __create_poststation_therapy_end_dict(date_end: str, days: str) -> dict:
        """
        Date information in csv is missing hours and minutes, so dummy values are added
        """
        date_end = CSVObservationFactConverter._convert_date_to_i2b2_format(''.join([date_end, '0000']))
        if days:
            return {'concept_cd': 'P21:POSTDIS', 'start_date': date_end, 'modifier_cd': '@', 'valtype_cd': 'N', 'nval_num': days, 'units_cd': 'd'}
        else:
            return {'concept_cd': 'P21:POSTDIS', 'start_date': date_end, 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'}

    def create_script_rows(self) -> list:
        return [{'concept_cd': 'P21:SCRIPT', 'modifier_cd': '@', 'valtype_cd': '@', 'valueflag_cd': '@'},
                {'concept_cd': 'P21:SCRIPT', 'modifier_cd': 'scriptVer', 'valtype_cd': 'T', 'tval_char': self.SCRIPT_VERSION},
                {'concept_cd': 'P21:SCRIPT', 'modifier_cd': 'scriptId', 'valtype_cd': 'T', 'tval_char': self.SCRIPT_ID}]


class FABObservationFactConverter(CSVObservationFactConverter):
    """
    In fab.csv, all columns but 'fabentlassungsdatum' are mandatory.
    """

    def __init__(self):
        super().__init__()
        self.COUNTER_INSTANCE = ObservationFactInstanceCounter()

    def create_observation_facts_from_row(self, row_csv: pd.Series) -> list:
        id_case = row_csv['khinterneskennzeichen']
        num_instance = self.COUNTER_INSTANCE.add_row_instance_count(id_case)
        facts = self.__create_department_dict(num_instance, row_csv['fachabteilung'], row_csv['kennungintensivbett'], row_csv['fabaufnahmedatum'], row_csv['fabentlassungsdatum'])
        return [facts]

    def __create_department_dict(self, num_instance: str, department: str, intensive: str, date_start: str, date_end: str) -> dict:
        date_start = self._convert_date_to_i2b2_format(date_start)
        date_end = self._convert_date_to_i2b2_format(date_end) if date_end else None
        concept = 'P21:DEP:CC' if intensive == 'J' else 'P21:DEP'
        return {'concept_cd': concept, 'start_date': date_start, 'modifier_cd': '@', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': department, 'end_date': date_end}


class ICDObservationFactConverter(CSVObservationFactConverter):
    """
    In icd.csv, only the columns 'icdkode','icdversion' and 'diagnoseart' are mandatory.
    Other columns may be empty and are only added, if the columns contains a value.
    """

    def __init__(self):
        super().__init__()
        self.COUNTER_INSTANCE = ObservationFactInstanceCounter()

    def create_observation_facts_from_row(self, row_csv: pd.Series) -> list:
        facts = []
        id_case = row_csv['khinterneskennzeichen']
        num_instance = self.COUNTER_INSTANCE.add_row_instance_count(id_case)
        facts.extend(self.__create_icd_dicts(num_instance, row_csv['icdkode'], row_csv['diagnoseart'], row_csv['icdversion'], row_csv['lokalisation'], row_csv['diagnosensicherheit']))
        if row_csv['sekundärkode']:
            num_instance = self.COUNTER_INSTANCE.add_row_instance_count(id_case)
            facts.extend(
                    self.__create_icd_sek_dicts(num_instance, row_csv['sekundärkode'], row_csv['icdkode'], row_csv['icdversion'], row_csv['sekundärlokalisation'], row_csv['sekundärdiagnosensicherheit']))
        return facts

    def __create_icd_dicts(self, num_instance: str, code: str, type_diag: str, version: str, localisation: str, certainty: str) -> list:
        concept = ':'.join(['ICD10GM', self.__convert_icd_code_to_i2b2_format(code)])
        list_facts = [{'concept_cd': concept, 'modifier_cd': '@', 'instance_num': num_instance, 'valtype_cd': '@', 'valueflag_cd': '@'},
                      {'concept_cd': concept, 'modifier_cd': 'diagType', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': type_diag},
                      {'concept_cd': concept, 'modifier_cd': 'cdVersion', 'instance_num': num_instance, 'valtype_cd': 'N', 'nval_num': version, 'units_cd': 'yyyy'}]
        if localisation:
            list_facts.append({'concept_cd': concept, 'modifier_cd': 'localisation', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': localisation})
        if certainty:
            list_facts.append({'concept_cd': concept, 'modifier_cd': 'certainty', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': certainty})
        return list_facts

    def __create_icd_sek_dicts(self, num_instance: str, code: str, code_parent: str, version: str, localisation: str, certainty: str) -> list:
        list_facts = self.__create_icd_dicts(num_instance, code, 'SD', version, localisation, certainty)
        concept_parent = ':'.join(['ICD10GM', self.__convert_icd_code_to_i2b2_format(code_parent)])
        concept_icd = ':'.join(['ICD10GM', self.__convert_icd_code_to_i2b2_format(code)])
        list_facts.append({'concept_cd': concept_icd, 'modifier_cd': 'sdFrom', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': concept_parent})
        return list_facts

    @staticmethod
    def __convert_icd_code_to_i2b2_format(code) -> str:
        """
        Converts icd code to i2b2crcdata.observation_fact format. Example:
        F2424 -> F24.24
        F24.24 -> F24.24
        J90 -> J90
        J21. -> J21.
        """
        if len(code) > 3:
            code = ''.join([code[:3], '.', code[3:]] if code[3] != '.' else code)
        return code


class OPSObservationFactConverter(CSVObservationFactConverter):
    """
    In ops.csv, all columns but 'lokalisation' are mandatory.
    """

    def __init__(self):
        super().__init__()
        self.COUNTER_INSTANCE = ObservationFactInstanceCounter()

    def create_observation_facts_from_row(self, row_csv: pd.Series) -> list:
        id_case = row_csv['khinterneskennzeichen']
        num_instance = self.COUNTER_INSTANCE.add_row_instance_count(id_case)
        return self.__create_ops_dicts(num_instance, row_csv['opskode'], row_csv['opsversion'], row_csv['lokalisation'], row_csv['opsdatum'])

    def __create_ops_dicts(self, num_instance: str, code, version, localisation, date: str) -> list:
        date = self._convert_date_to_i2b2_format(date)
        concept = ':'.join(['OPS', self.__convert_ops_code_to_i2b2_format(code)])
        list_facts = [{'concept_cd': concept, 'start_date': date, 'modifier_cd': '@', 'instance_num': num_instance, 'valtype_cd': '@', 'valueflag_cd': '@'},
                      {'concept_cd': concept, 'start_date': date, 'modifier_cd': 'cdVersion', 'instance_num': num_instance, 'valtype_cd': 'N', 'nval_num': version, 'units_cd': 'yyyy'}]
        if localisation:
            list_facts.append({'concept_cd': concept, 'start_date': date, 'modifier_cd': 'localisation', 'instance_num': num_instance, 'valtype_cd': 'T', 'tval_char': localisation})
        return list_facts

    @staticmethod
    def __convert_ops_code_to_i2b2_format(code) -> str:
        """
        Converts ops code to i2b2crcdata.observation_fact format. Example:
        964922 -> 9-649.22
        9-64922 -> 9-649.22
        9649.22 -> 9-649.22
        9-649.22 -> 9-649.22
        1-5020 -> 1-502.0
        1-501 -> 1-501
        1051 -> 1-501
        """
        code = ''.join([code[:1], '-', code[1:]] if code[1] != '-' else code)
        if len(code) > 5:
            code = ''.join([code[:5], '.', code[5:]] if code[5] != '.' else code)
        return code


class ObservationFactInstanceCounter:
    """
    Helper class for CSVObservationFactConverter.
    Is used to keep track of reappearing encounter in a csv file
    """

    def __init__(self):
        self.DICT_NUM_INSTANCES = {}

    def add_row_instance_count(self, id_case: str) -> str:
        if id_case not in self.DICT_NUM_INSTANCES:
            self.DICT_NUM_INSTANCES[id_case] = 1
        else:
            self.DICT_NUM_INSTANCES[id_case] += 1
        return self.DICT_NUM_INSTANCES.get(id_case)


class DatabaseConnection(ABC):
    ENGINE: db.engine.Engine = None

    def __init__(self):
        self.USERNAME = os.environ['username']
        self.PASSWORD = os.environ['password']
        self.I2B2_CONNECTION_URL = os.environ['connection-url']
        self.__init_engine()

    def __init_engine(self):
        pattern = r'jdbc:postgresql://(.*?)(\?searchPath=.*)?$'
        connection = re.search(pattern, self.I2B2_CONNECTION_URL).group(1)
        self.ENGINE = db.create_engine(f"postgresql+psycopg2://{self.USERNAME}:{self.PASSWORD}@{connection}", pool_pre_ping=True)

    def open_connection(self):
      return self.ENGINE.connect()

    def __del__(self):
      if self.ENGINE is not None:
        self.ENGINE.dispose()


class DatabaseExtractor(DatabaseConnection, ABC):
    SIZE_CHUNKS: int = 10000

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        pass

    def _stream_query_into_df(self, query: db.sql.expression) -> pd.DataFrame:
        df = pd.DataFrame()
        with self.open_connection() as conn:
          result = conn.execution_options(stream_results=True).execute(query)
          while True:
            chunk = result.fetchmany(self.SIZE_CHUNKS)
            if not chunk:
              break
            if df.empty:
              df = pd.DataFrame(chunk)
            else:
              df = df.append(chunk, ignore_index=True)
          if df.empty:
            raise ValueError("No entries for database query was found")
          df.columns = result.keys()
          return df


class EncounterInfoExtractorWithEncounterId(DatabaseExtractor):
    """
    SQLAlchemy-Query to extract encounter_id, encounter_num and patient_num for AKTIN
    optin encounter from database. Column for encounter_id is renmaed to 'match_id'
    to streamline the matching in DatabaseEncounterMatcher.
    """

    def extract(self) -> pd.DataFrame:
          enc = db.Table(
              "encounter_mapping", db.MetaData(), autoload_with=self.ENGINE
          )
          pat = db.Table("patient_mapping", db.MetaData(), autoload_with=self.ENGINE)
          opt = db.Table(
              "optinout_patients", db.MetaData(), autoload_with=self.ENGINE
          )
          query = (
              db.select(
                  enc.c["encounter_ide"],
                  enc.c["encounter_num"],
                  pat.c["patient_num"],
              )
              .select_from(
                  enc.join(pat, enc.c["patient_ide"] == pat.c["patient_ide"]).join(
                      opt, pat.c["patient_ide"] == opt.c["pat_psn"], isouter=True
                  )
              )
              .where(db.or_(opt.c["study_id"] != "AKTIN", opt.c["pat_psn"].is_(None)))
          )
          df = self._stream_query_into_df(query)
          df.rename(columns={"encounter_ide": "match_id"}, inplace=True)
          return df


class EncounterInfoExtractorWithBillingId(DatabaseExtractor):
    """
    SQLAlchemy-Query to extract billing_id, encounter_num and patient_num for AKTIN
    optin encounter from database. Column for billing_id is renmaed to 'match_id'
    to streamline the matching in DatabaseEncounterMatcher.
    """

    def extract(self) -> pd.DataFrame:
          fact = db.Table(
              "observation_fact", db.MetaData(), autoload_with=self.ENGINE
          )
          pat = db.Table("patient_mapping", db.MetaData(), autoload_with=self.ENGINE)
          opt = db.Table(
              "optinout_patients", db.MetaData(), autoload_with=self.ENGINE
          )
          query = (
              db.select(
                  fact.c["tval_char"],
                  fact.c["encounter_num"],
                  fact.c["patient_num"],
              )
              .select_from(
                  fact.join(pat, fact.c["patient_num"] == pat.c["patient_num"]).join(
                      opt, pat.c["patient_ide"] == opt.c["pat_psn"], isouter=True
                  )
              )
              .where(
                  db.and_(
                      db.or_(
                          opt.c["study_id"] != "AKTIN", opt.c["pat_psn"].is_(None)
                      ),
                      fact.c["concept_cd"] == "AKTIN:Fallkennzeichen",
                  )
              )
          )
          df = self._stream_query_into_df(query)
          df.rename(columns={"tval_char": "match_id"}, inplace=True)
          return df


class DatabaseEncounterMatcher:
    """
    Matches a list of encounter ids from a csv file (column 'khinterneskennzeichen')
    with ids from the database. Type of matching is determined by the given instance
    of DatabaseExtractor (encounter id or billing id).
    """

    def __init__(self, extractor: DatabaseExtractor):
        self.READER = AktinPropertiesReader()
        algorithm = self.READER.get_property('pseudonym.algorithm')
        self.ANONYMIZER = OneWayAnonymizer(algorithm)
        self.EXTRACTOR = extractor

    def get_matched_df(self, list_csv_ids: list) -> pd.DataFrame:
        """
        Matches input list of csv ids with ids from database.
        Returns a dataframe with unhashed encounter id (from csv) and corresponding
        patient num and encounter num (from database). This dataframe is merged
        together with the output of FALLVerifier.get_unique_ids_of_valid_encounter_with_admission_dates()
        to create the mapping dataframe required by CSVObservationFactUploadManager.
        """
        salt = self.__get_salt_property()
        root = self.__get_extractor_type_root()
        df_db = self.EXTRACTOR.extract()
        list_csv_ide = self.ANONYMIZER.anonymize_list(root, list_csv_ids, salt)
        df_csv = pd.DataFrame(list(zip(list_csv_ids, list_csv_ide)), columns=['encounter_id', 'match_id'])
        df_merged = pd.merge(df_db, df_csv, on=['match_id'])
        df_merged = df_merged.drop(['match_id'], axis=1)
        if df_merged.empty:
            raise SystemExit('no encounter could be matched with database')
        return df_merged

    def __get_extractor_type_root(self) -> str:
        if isinstance(self.EXTRACTOR, EncounterInfoExtractorWithEncounterId):
            return self.READER.get_property('cda.encounter.root.preset')
        elif isinstance(self.EXTRACTOR, EncounterInfoExtractorWithBillingId):
            return self.READER.get_property('cda.billing.root.preset')
        else:
            raise SystemExit('invalid instance of DatabaseExtractor')

    def __get_salt_property(self) -> str:
        return self.READER.get_property('pseudonym.salt')


class AktinPropertiesReader:

    def __init__(self):
        self.PATH_AKTIN_PROPERTIES = os.environ['path_aktin_properties']
        if not os.path.exists(self.PATH_AKTIN_PROPERTIES):
            raise SystemExit('file path for aktin.properties is not valid')

    def get_property(self, prop: str) -> str:
        with open(self.PATH_AKTIN_PROPERTIES) as properties:
            for line in properties:
                if '=' in line:
                    key, value = line.split('=', 1)
                    if key == prop:
                        return value.strip()
            return ''


class OneWayAnonymizer:
    """
    Same hashing process as the AKTIN DWH
    """

    def __init__(self, name_alg):
        name_alg = name_alg or 'sha1'
        self.ALGORITHM = self.__convert_crypto_alg_name(name_alg)

    @staticmethod
    def __convert_crypto_alg_name(name_alg: str) -> str:
        """
        Converts given name of java cryptograhpic hash function to python demanted format
        """
        return str.lower(name_alg.replace('-', '', ).replace('/', '_'))

    def anonymize(self, root, ext, salt) -> str:
        composite = '/'.join([str(root), str(ext)])
        composite = salt + composite if salt else composite
        buffer = composite.encode('UTF-8')
        alg = getattr(hashlib, self.ALGORITHM)()
        alg.update(buffer)
        return base64.urlsafe_b64encode(alg.digest()).decode('UTF-8')

    def anonymize_list(self, root, list_ext, salt) -> list:
        return [self.anonymize(root, ext, salt) for ext in list_ext]


class TableHandler(DatabaseConnection, ABC):
    """
    Interface for i2b2 tables.
    Table must be reflected prior uploading/deleting data.
    """
    TABLE: db.schema.Table = None

    @abstractmethod
    def reflect_table(self):
        pass

    @abstractmethod
    def upload_data(self, list_dicts: list):
        pass

    @abstractmethod
    def delete_data(self, identifier: str):
        pass


class ObservationFactTableHandler(TableHandler):
    """
    Uploads data to/deletes data from i2b2crcdata.observation_fact
    """

    def reflect_table(self):
        self.TABLE = db.Table('observation_fact', db.MetaData(), autoload_with=self.ENGINE)

    def upload_data(self, list_dicts: list):
        with self.open_connection() as conn:
            with conn.begin() as transaction:
                try:
                    conn.execute(self.TABLE.insert(), list_dicts)
                except exc.SQLAlchemyError:
                    transaction.rollback()
                    traceback.print_exc()
                    raise SystemExit("Upload operation failed")

    def delete_data(self, identifier: str):
      sourcesystem = self.__get_sourcesystem_of_encounter(identifier)
      if self.__is_sourcesystem_valid(sourcesystem):
        sourcesystem_cd = sourcesystem[0][0]
        with self.open_connection() as conn:
          with conn.begin() as transaction:
            try:
              statement_delete = (
                self.TABLE.delete()
                .where(self.TABLE.c["encounter_num"] == str(identifier))
                .where(self.TABLE.c["sourcesystem_cd"] == sourcesystem_cd)
              )
              conn.execute(statement_delete)
            except exc.SQLAlchemyError:
              transaction.rollback()
              traceback.print_exc()
              raise SystemExit("delete operation for encounter failed")

    def check_if_encounter_is_imported(self, num_enc: str) -> bool:
        sourcesystem = self.__get_sourcesystem_of_encounter(num_enc)
        return self.__is_sourcesystem_valid(sourcesystem)

    def __get_sourcesystem_of_encounter(self, num_enc: str) -> str:
        """
        Checks, if an enconter was already uploaded using this script (including older versions).
        Check is done by matching the observation fact rows of script metadata (see
        FALLObservationFactConverter) of the corresponding encounter with the metadata
        of this script.
        """
        with self.open_connection() as conn:
          query = (
            db.select(self.TABLE.c["sourcesystem_cd"])
            .where(self.TABLE.c["encounter_num"] == str(num_enc))
            .where(self.TABLE.c["concept_cd"] == "P21:SCRIPT")
            .where(self.TABLE.c["modifier_cd"] == "scriptId")
            .where(self.TABLE.c["provider_id"] == "P21")
          )
          return conn.execute(query).fetchall()

    @staticmethod
    def __is_sourcesystem_valid(sourcesystem: str) -> bool:
        if sourcesystem:
            if len(sourcesystem) != 1:
                raise SystemExit('invalid number of sourcesystems for encounter found')
            return True
        return False


class CSVObservationFactUploadManager(ABC):
    """
    Uploads all valid encounter data of a given csv file as observation facts to i2b2crcdata.observation_fact.
    Needs a mapping table to map the unhashed ids of the csv file with the patient_num and encounter_num in
    database. Values for 'aufnahmedatum' are also needed as a default value for 'start_date' in i2b2 table
    (see CSVObservationFactConverter.add_static_values_to_row_dict()).
    """
    VERIFIER: CSVFileVerifier
    CONVERTER: CSVObservationFactConverter

    def __init__(self, matched_encounter_info: pd.DataFrame):
        self.TABLEHANDLER: ObservationFactTableHandler = ObservationFactTableHandler()
        self.DF_MAPPING = matched_encounter_info
        if self.DF_MAPPING.empty:
            raise SystemExit('given encounter mapping dataframe is empty')
        if not {'encounter_id', 'encounter_num', 'patient_num', 'aufnahmedatum'}.issubset(self.DF_MAPPING.columns):
            raise SystemExit('invalid encounter mapping dataframe supplied')

    def upload_csv(self):
      self.TABLEHANDLER.reflect_table()
      for chunk in pd.read_csv(
          self.VERIFIER.PATH_CSV,
          chunksize=self.VERIFIER.SIZE_CHUNKS,
          sep=self.VERIFIER.CSV_SEPARATOR,
          encoding=self.VERIFIER.get_csv_encoding(),
          dtype=str,
      ):
        chunk = self._clear_chunk_from_invalid_data(chunk)
        if chunk.empty:
          continue
        list_observation_fact_dicts = self._convert_chunk_to_uploadable_facts(
            chunk
        )
        self.TABLEHANDLER.upload_data(list_observation_fact_dicts)

    def _clear_chunk_from_invalid_data(self, chunk: pd.Series) -> pd.Series:
        chunk = chunk[list(self.VERIFIER.DICT_COLUMN_PATTERN.keys())]
        chunk = chunk[chunk['khinterneskennzeichen'].isin(self.DF_MAPPING['encounter_id'])]
        chunk = chunk.fillna('')
        for column in chunk.columns.values:
            chunk = self.VERIFIER.clear_invalid_column_fields_in_chunk(chunk, column)
        return chunk

    def _convert_chunk_to_uploadable_facts(self, chunk: pd.Series) -> list:
        list_observation_fact_dicts = []
        for row_csv in chunk.iterrows():
            row_csv = row_csv[1]
            list_converted_row = self.CONVERTER.create_observation_facts_from_row(row_csv)
            list_converted_row = self._add_static_observation_facts(list_converted_row, row_csv['khinterneskennzeichen'])
            list_observation_fact_dicts.extend(list_converted_row)
        return list_observation_fact_dicts

    def _add_static_observation_facts(self, list_facts: list, id_case: str) -> list:
        row_case = self.DF_MAPPING.loc[self.DF_MAPPING['encounter_id'] == id_case]
        num_enc = str(row_case['encounter_num'].values[0])
        num_pat = str(row_case['patient_num'].values[0])
        date_admission = row_case['aufnahmedatum'].values[0]
        for index, row in enumerate(list_facts):
            list_facts[index] = self.CONVERTER.add_static_values_to_row_dict(row, num_enc, num_pat, date_admission)
        return list_facts


class FALLObservationFactUploadManager(CSVObservationFactUploadManager):
    """
    Overrides _convert_chunk_to_uploadable_facts() to check and delete all p21 data of an encounter
    if it was already uploaded using this script.
    """

    def __init__(self, df_mapping: pd.DataFrame, path_folder: str):
        super().__init__(df_mapping)
        self.VERIFIER = FALLVerifier(path_folder)
        self.CONVERTER = FALLObservationFactConverter()
        self.NUM_IMPORTS = 0
        self.NUM_UPDATES = 0

    def _convert_chunk_to_uploadable_facts(self, chunk: pd.Series) -> list:
        list_observation_fact_dicts = []
        for row_csv in chunk.iterrows():
            row_csv = row_csv[1]
            num_enc = str(self.DF_MAPPING.loc[self.DF_MAPPING['encounter_id'] == row_csv['khinterneskennzeichen']]['encounter_num'].values[0])
            if self.TABLEHANDLER.check_if_encounter_is_imported(num_enc):
                self.TABLEHANDLER.delete_data(num_enc)
                self.NUM_UPDATES += 1
            else:
                self.NUM_IMPORTS += 1
            list_converted_row = self.CONVERTER.create_observation_facts_from_row(row_csv)
            list_converted_row.extend(self.CONVERTER.create_script_rows())
            list_converted_row = self._add_static_observation_facts(list_converted_row, row_csv['khinterneskennzeichen'])
            list_observation_fact_dicts.extend(list_converted_row)
        return list_observation_fact_dicts


class FABObservationFactUploadManager(CSVObservationFactUploadManager):
    def __init__(self, df_mapping: pd.DataFrame, path_folder: str):
        super().__init__(df_mapping)
        self.VERIFIER = FABVerifier(path_folder)
        self.CONVERTER = FABObservationFactConverter()


class ICDObservationFactUploadManager(CSVObservationFactUploadManager):
    def __init__(self, df_mapping: pd.DataFrame, path_folder: str):
        super().__init__(df_mapping)
        self.VERIFIER = ICDVerifier(path_folder)
        self.CONVERTER = ICDObservationFactConverter()


class OPSObservationFactUploadManager(CSVObservationFactUploadManager):
    def __init__(self, df_mapping: pd.DataFrame, path_folder: str):
        super().__init__(df_mapping)
        self.VERIFIER = OPSVerifier(path_folder)
        self.CONVERTER = OPSObservationFactConverter()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('Sys.argv don\'t match')
    p21 = P21Importer(sys.argv[1])
    p21.import_file()

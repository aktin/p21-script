# -*- coding: UTF-8 -*-.

import unittest
import os
import pandas as pd
import chardet
from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor
from src.p21import import FABPreprocessor
from src.p21import import FABVerifier


def get_csv_encoding(path_csv: str) -> str:
    with open(path_csv, 'rb') as csv:
        encoding = chardet.detect(csv.read(1024))['encoding']
    return encoding


def get_all_case_ids_as_set(verifier: FABVerifier) -> set:
    df = pd.read_csv(verifier.PATH_CSV, sep=verifier.CSV_SEPARATOR, encoding=get_csv_encoding(verifier.PATH_CSV), dtype=str)
    return set(df['khinterneskennzeichen'].unique())


class TestFABVerifier(unittest.TestCase):

    def setUp(self) -> None:
        path_parent = os.path.dirname(os.getcwd())
        path_resources = os.path.join(path_parent, 'resources')
        path_zip = os.path.join(path_resources, 'p21_verification.zip')
        self.TMP = TmpFolderManager(path_resources)
        zfe = ZipFileExtractor(path_zip)
        self.PATH_TMP = self.TMP.create_tmp_folder()
        zfe.extract_zip_to_folder(self.PATH_TMP)

    def tearDown(self) -> None:
        self.TMP.remove_tmp_folder()

    def test_is_fab_in_csv_folder(self):
        fab = FABVerifier(self.PATH_TMP)
        self.assertTrue(fab.is_csv_in_folder())

    def test_is_wrong_fab_in_csv_folder(self):
        FABVerifier.CSV_NAME = 'fab2.csv'
        fab = FABVerifier(self.PATH_TMP)
        self.assertFalse(fab.is_csv_in_folder())
        FABVerifier.CSV_NAME = 'fab.csv'

    def test_check_column_names_of_fab(self):
        preprocessor = FABPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fab = FABVerifier(self.PATH_TMP)
        fab.check_column_names_of_csv()

    def test_check_missing_mandatory_column_names_of_fab(self):
        FABPreprocessor.CSV_NAME = 'fab_missing_cols.csv'
        FABVerifier.CSV_NAME = 'fab_missing_cols.csv'
        preprocessor = FABPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fab = FABVerifier(self.PATH_TMP)
        with self.assertRaises(SystemExit):
            fab.check_column_names_of_csv()
        FABPreprocessor.CSV_NAME = 'fab.csv'
        FABVerifier.CSV_NAME = 'fab.csv'

    def test_check_missing_optional_column_names_of_fab(self):
        FABPreprocessor.CSV_NAME = 'fab_no_optional_cols.csv'
        FABVerifier.CSV_NAME = 'fab_no_optional_cols.csv'
        preprocessor = FABPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fab = FABVerifier(self.PATH_TMP)
        fab.check_column_names_of_csv()
        FABPreprocessor.CSV_NAME = 'fab.csv'
        FABVerifier.CSV_NAME = 'fab.csv'

    def test_get_ids_of_valid_fab_cases(self):
        preprocessor = FABPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fab = FABVerifier(self.PATH_TMP)
        set_valid = set(fab.get_unique_ids_of_valid_encounter())
        set_all_ids = get_all_case_ids_as_set(fab)
        set_invalid = set_all_ids.difference(set_valid)
        set_required_invalid = {'1021', '1022', '1023'}
        self.assertEqual(set_required_invalid, set_invalid)

    def test_get_ids_of_valid_fab_cases_in_empty_df(self):
        FABPreprocessor.CSV_NAME = 'fab_empty.csv'
        FABVerifier.CSV_NAME = 'fab_empty.csv'
        preprocessor = FABPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fab = FABVerifier(self.PATH_TMP)
        self.assertEqual([], fab.get_unique_ids_of_valid_encounter())
        FABPreprocessor.CSV_NAME = 'fab.csv'
        FABVerifier.CSV_NAME = 'fab.csv'

    def test_clear_invalid_fields_in_fachabteilung(self):
        fab = FABVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'fachabteilung': ['HA1024', 'BA1024', 'BE1024', 'BC1024', 'HA24', 'HA123456', '']})
        chunk = fab.clear_invalid_column_fields_in_chunk(chunk, 'fachabteilung')
        self.assertEqual(3, len(chunk['fachabteilung']))

    def test_clear_invalid_fields_in_fabaufnahmedatum(self):
        fab = FABVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'fabaufnahmedatum': ['202001010000', '202031122359', '20200101', 'MISSING', '202001010000.0000', '2020010100000000', '']})
        chunk = fab.clear_invalid_column_fields_in_chunk(chunk, 'fabaufnahmedatum')
        self.assertEqual(2, len(chunk['fabaufnahmedatum']))

    def test_clear_invalid_fields_in_fabentlassungsdatum(self):
        fab = FABVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'fabentlassungsdatum': ['202001010000', '202031122359', '20200101', 'MISSING', '202001010000.0000', '2020010100000000', '']})
        chunk = fab.clear_invalid_column_fields_in_chunk(chunk, 'fabentlassungsdatum')
        self.assertEqual(7, len(chunk['fabentlassungsdatum']))
        self.assertEqual(5, (len(chunk[chunk['fabentlassungsdatum'] == ''])))

    def test_clear_invalid_fields_in_kennungintensivbett(self):
        fab = FABVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'kennungintensivbett': ['N', 'J', 'Y', 'Yes', 'No', '1', '0', '']})
        chunk = fab.clear_invalid_column_fields_in_chunk(chunk, 'kennungintensivbett')
        self.assertEqual(2, len(chunk['kennungintensivbett']))

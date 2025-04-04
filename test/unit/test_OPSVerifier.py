# -*- coding: UTF-8 -*-.

import unittest
import os
import pandas as pd

from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor
from src.p21import import OPSPreprocessor
from src.p21import import OPSVerifier


def get_all_case_ids_as_set(verifier: OPSVerifier) -> set:
    df = pd.read_csv(verifier.PATH_CSV, sep=verifier.CSV_SEPARATOR, encoding='utf-8', dtype=str)
    return set(df['khinterneskennzeichen'].unique())


class TestOPSVerifier(unittest.TestCase):

    def setUp(self) -> None:
        path_parent = os.path.dirname(os.getcwd())
        path_resources = os.path.join(path_parent, 'resources')
        path_zip = os.path.join(path_resources, 'p21_verification.zip')
        self.TMP = TmpFolderManager(path_resources)
        zfe = ZipFileExtractor(path_zip)
        self.PATH_TMP = self.TMP.create_tmp_folder()
        zfe.extract_zip_to_folder(self.PATH_TMP)
        self.TMP.rename_files_in_tmp_folder_to_lowercase()

    def tearDown(self) -> None:
        self.TMP.remove_tmp_folder()

    def test_is_ops_in_csv_folder(self):
        ops = OPSVerifier(self.PATH_TMP)
        self.assertTrue(ops.is_csv_in_folder())

    def test_is_wrong_ops_in_csv_folder(self):
        OPSVerifier.CSV_NAME = 'ops2.csv'
        ops = OPSVerifier(self.PATH_TMP)
        self.assertFalse(ops.is_csv_in_folder())
        OPSVerifier.CSV_NAME = 'ops.csv'

    def test_check_column_names_of_ops(self):
        preprocessor = OPSPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        ops = OPSVerifier(self.PATH_TMP)
        ops.check_column_names_of_csv()

    def test_check_missing_mandatory_column_names_of_ops(self):
        OPSPreprocessor.CSV_NAME = 'ops_missing_cols.csv'
        OPSVerifier.CSV_NAME = 'ops_missing_cols.csv'
        preprocessor = OPSPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        ops = OPSVerifier(self.PATH_TMP)
        with self.assertRaises(SystemExit):
            ops.check_column_names_of_csv()
        OPSPreprocessor.CSV_NAME = 'ops.csv'
        OPSVerifier.CSV_NAME = 'ops.csv'

    def test_check_missing_optional_column_names_of_ops(self):
        OPSPreprocessor.CSV_NAME = 'ops_no_optional_cols.csv'
        OPSVerifier.CSV_NAME = 'ops_no_optional_cols.csv'
        preprocessor = OPSPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        ops = OPSVerifier(self.PATH_TMP)
        ops.check_column_names_of_csv()
        OPSPreprocessor.CSV_NAME = 'ops.csv'
        OPSVerifier.CSV_NAME = 'ops.csv'

    def test_get_ids_of_valid_ops_cases(self):
        preprocessor = OPSPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        ops = OPSVerifier(self.PATH_TMP)
        set_valid = set(ops.get_unique_ids_of_valid_encounter())
        set_all_ids = get_all_case_ids_as_set(ops)
        set_invalid = set_all_ids.difference(set_valid)
        set_required_invalid = {'1021', '1022', '1023'}
        self.assertEqual(set_required_invalid, set_invalid)

    def test_get_ids_of_valid_ops_cases_in_empty_df(self):
        OPSPreprocessor.CSV_NAME = 'ops_empty.csv'
        OPSVerifier.CSV_NAME = 'ops_empty.csv'
        preprocessor = OPSPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        ops = OPSVerifier(self.PATH_TMP)
        self.assertEqual([], ops.get_unique_ids_of_valid_encounter())
        OPSPreprocessor.CSV_NAME = 'ops.csv'
        OPSVerifier.CSV_NAME = 'ops.csv'

    def test_clear_invalid_fields_in_opsversion(self):
        ops = OPSVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'opsversion': ['2019', '2020', '2021', '1999', 'ABCD', '20200', '20a0', '']})
        chunk = ops.clear_invalid_column_fields_in_chunk(chunk, 'opsversion')
        self.assertEqual(3, len(chunk['opsversion']))

    def test_clear_invalid_fields_in_opskode(self):
        ops = OPSVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'opskode': ['964922', '9-64922', '9649.22', '9-649.22', '1-501', '1-5020', '1051', 'A1-501', 'ABCD', 'A11.123456', 'F2424', 'G25.25', 'J90', 'J21.', 'V97.33XD',
                                          'V0001XD', 'Y93D', '6A66', 'A11.123456', 'AA2.22', '']})
        chunk = ops.clear_invalid_column_fields_in_chunk(chunk, 'opskode')
        self.assertEqual(7, len(chunk['opskode']))

    def test_clear_invalid_fields_in_opsdatum(self):
        ops = OPSVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'opsdatum': ['202001010000', '202031122359', '20200101', 'MISSING', '202001010000.0000', '2020010100000000', '']})
        chunk = ops.clear_invalid_column_fields_in_chunk(chunk, 'opsdatum')
        self.assertEqual(2, len(chunk['opsdatum']))

    def test_clear_invalid_fields_in_lokalisation(self):
        ops = OPSVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'lokalisation': ['B', 'L', 'R', 'l', 'r', 'X', '1', 'BL', '']})
        chunk = ops.clear_invalid_column_fields_in_chunk(chunk, 'lokalisation')
        self.assertEqual(9, len(chunk['lokalisation']))
        self.assertEqual(6, (len(chunk[chunk['lokalisation'] == ''])))

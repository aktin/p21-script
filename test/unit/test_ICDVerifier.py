# -*- coding: UTF-8 -*-.

import unittest
import os
import pandas as pd

from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor
from src.p21import import ICDPreprocessor
from src.p21import import ICDVerifier


def get_all_case_ids_as_set(verifier: ICDVerifier) -> set:
    df = pd.read_csv(verifier.PATH_CSV, sep=verifier.CSV_SEPARATOR, encoding='utf-8', dtype=str)
    return set(df['khinterneskennzeichen'].unique())


class TestICDVerifier(unittest.TestCase):

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

    def test_is_icd_in_csv_folder(self):
        icd = ICDVerifier(self.PATH_TMP)
        self.assertTrue(icd.is_csv_in_folder())

    def test_is_wrong_icd_in_csv_folder(self):
        ICDVerifier.CSV_NAME = 'icd2.csv'
        icd = ICDVerifier(self.PATH_TMP)
        self.assertFalse(icd.is_csv_in_folder())
        ICDVerifier.CSV_NAME = 'icd.csv'

    def test_check_column_names_of_icd(self):
        preprocessor = ICDPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        icd = ICDVerifier(self.PATH_TMP)
        icd.check_column_names_of_csv()

    def test_check_missing_mandatory_column_names_of_icd(self):
        ICDPreprocessor.CSV_NAME = 'icd_missing_cols.csv'
        ICDVerifier.CSV_NAME = 'icd_missing_cols.csv'
        preprocessor = ICDPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        icd = ICDVerifier(self.PATH_TMP)
        with self.assertRaises(SystemExit):
            icd.check_column_names_of_csv()
        ICDPreprocessor.CSV_NAME = 'icd.csv'
        ICDVerifier.CSV_NAME = 'icd.csv'

    def test_check_missing_optional_column_names_of_icd(self):
        ICDPreprocessor.CSV_NAME = 'icd_no_optional_cols.csv'
        ICDVerifier.CSV_NAME = 'icd_no_optional_cols.csv'
        preprocessor = ICDPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        icd = ICDVerifier(self.PATH_TMP)
        icd.check_column_names_of_csv()
        ICDPreprocessor.CSV_NAME = 'icd.csv'
        ICDVerifier.CSV_NAME = 'icd.csv'

    def test_get_ids_of_valid_icd_cases(self):
        preprocessor = ICDPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        icd = ICDVerifier(self.PATH_TMP)
        set_valid = set(icd.get_unique_ids_of_valid_encounter())
        set_all_ids = get_all_case_ids_as_set(icd)
        set_invalid = set_all_ids.difference(set_valid)
        set_required_invalid = {'1021', '1022', '1023'}
        self.assertEqual(set_required_invalid, set_invalid)

    def test_get_ids_of_valid_icd_cases_in_empty_df(self):
        ICDPreprocessor.CSV_NAME = 'icd_empty.csv'
        ICDVerifier.CSV_NAME = 'icd_empty.csv'
        preprocessor = ICDPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        icd = ICDVerifier(self.PATH_TMP)
        self.assertEqual([], icd.get_unique_ids_of_valid_encounter())
        ICDPreprocessor.CSV_NAME = 'icd.csv'
        ICDVerifier.CSV_NAME = 'icd.csv'

    def test_check_column_names_of_icd_with_sek(self):
        ICDPreprocessor.CSV_NAME = 'icd_with_sek.csv'
        ICDVerifier.CSV_NAME = 'icd_with_sek.csv'
        preprocessor = ICDPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        icd = ICDVerifier(self.PATH_TMP)
        icd.check_column_names_of_csv()
        ICDPreprocessor.CSV_NAME = 'icd.csv'
        ICDVerifier.CSV_NAME = 'icd.csv'

    def test_check_column_names_of_icd_without_sek(self):
        ICDPreprocessor.CSV_NAME = 'icd_no_sek.csv'
        ICDVerifier.CSV_NAME = 'icd_no_sek.csv'
        preprocessor = ICDPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        icd = ICDVerifier(self.PATH_TMP)
        icd.check_column_names_of_csv()
        ICDPreprocessor.CSV_NAME = 'icd.csv'
        ICDVerifier.CSV_NAME = 'icd.csv'

    def test_clear_invalid_fields_in_diagnoseart(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'diagnoseart': ['HD', 'ND', 'SD', 'SE', 'AB', 'HDD', '10', 'H1', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'diagnoseart')
        self.assertEqual(3, len(chunk['diagnoseart']))

    def test_clear_invalid_fields_in_icdversion(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'icdversion': ['2019', '2020', '2021', '1999', 'ABCD', '20200', '20a0', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'icdversion')
        self.assertEqual(3, len(chunk['icdversion']))

    def test_clear_invalid_fields_in_icdkode(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'icdkode': ['964922', '9-64922', '9649.22', '9-649.22', '1-501', '1-5020', '1051', 'A1-501', 'ABCD', 'A11.123456', 'F2424', 'G25.25', 'J90', 'J21.', 'V97.33XD',
                                          'V0001XD', 'Y93D', '6A66', 'A11.123456', 'AA2.22', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'icdkode')
        self.assertEqual(7, len(chunk['icdkode']))

    def test_clear_invalid_fields_in_lokalisation(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'lokalisation': ['B', 'L', 'R', 'l', 'r', 'X', '1', 'BL', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'lokalisation')
        self.assertEqual(9, len(chunk['lokalisation']))
        self.assertEqual(6, (len(chunk[chunk['lokalisation'] == ''])))

    def test_clear_invalid_fields_in_diagnosensicherheit(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'diagnosensicherheit': ['A', 'V', 'Z', 'G', 'a', 'Y', 'X', '1', 'VA', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'diagnosensicherheit')
        self.assertEqual(10, len(chunk['diagnosensicherheit']))
        self.assertEqual(6, (len(chunk[chunk['diagnosensicherheit'] == ''])))

    def test_clear_invalid_fields_in_sekundaerkode(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'sekundärkode': ['964922', '9-64922', '9649.22', '9-649.22', '1-501', '1-5020', '1051', 'A1-501', 'ABCD', 'A11.123456', 'F2424', 'G25.25', 'J90', 'J21.', 'V97.33XD',
                                               'V0001XD', 'Y93D', '6A66', 'A11.123456', 'AA2.22', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'sekundärkode')
        self.assertEqual(21, len(chunk['sekundärkode']))
        self.assertEqual(14, (len(chunk[chunk['sekundärkode'] == ''])))

    def test_clear_invalid_fields_in_sekundaerlokalisation(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'sekundärlokalisation': ['B', 'L', 'R', 'l', 'r', 'X', '1', 'BL', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'sekundärlokalisation')
        self.assertEqual(9, len(chunk['sekundärlokalisation']))
        self.assertEqual(6, (len(chunk[chunk['sekundärlokalisation'] == ''])))

    def test_clear_invalid_fields_in_sekundaerdiagnosensicherheit(self):
        icd = ICDVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'sekundärdiagnosensicherheit': ['A', 'V', 'Z', 'G', 'a', 'Y', 'X', '1', 'VA', '']})
        chunk = icd.clear_invalid_column_fields_in_chunk(chunk, 'sekundärdiagnosensicherheit')
        self.assertEqual(10, len(chunk['sekundärdiagnosensicherheit']))
        self.assertEqual(6, (len(chunk[chunk['sekundärdiagnosensicherheit'] == ''])))

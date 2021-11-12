# -*- coding: UTF-8 -*-.

import unittest
import os
import pandas as pd
import chardet
from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor
from src.p21import import FALLPreprocessor
from src.p21import import FALLVerifier


def get_csv_encoding(path_csv: str) -> str:
    with open(path_csv, 'rb') as csv:
        encoding = chardet.detect(csv.read(1024))['encoding']
    return encoding


def get_all_case_ids_as_set(verifier: FALLVerifier) -> set:
    df = pd.read_csv(verifier.PATH_CSV, sep=verifier.CSV_SEPARATOR, encoding=get_csv_encoding(verifier.PATH_CSV), dtype=str)
    return set(df['khinterneskennzeichen'].unique())


class TestFALLVerifier(unittest.TestCase):

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

    def test_is_fall_in_csv_folder(self):
        fall = FALLVerifier(self.PATH_TMP)
        self.assertTrue(fall.is_csv_in_folder())

    def test_is_wrong_fall_in_csv_folder(self):
        FALLVerifier.CSV_NAME = 'fall2.csv'
        fall = FALLVerifier(self.PATH_TMP)
        with self.assertRaises(SystemExit):
            fall.is_csv_in_folder()
        FALLVerifier.CSV_NAME = 'fall.csv'

    def test_check_column_names_of_fall(self):
        preprocessor = FALLPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fall = FALLVerifier(self.PATH_TMP)
        fall.check_column_names_of_csv()

    def test_check_missing_mandatory_column_names_of_fall(self):
        FALLPreprocessor.CSV_NAME = 'fall_missing_cols.csv'
        FALLVerifier.CSV_NAME = 'fall_missing_cols.csv'
        preprocessor = FALLPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fall = FALLVerifier(self.PATH_TMP)
        with self.assertRaises(SystemExit):
            fall.check_column_names_of_csv()
        FALLPreprocessor.CSV_NAME = 'fall.csv'
        FALLVerifier.CSV_NAME = 'fall.csv'

    def test_check_missing_optional_column_names_of_fall(self):
        FALLPreprocessor.CSV_NAME = 'fall_no_optional_cols.csv'
        FALLVerifier.CSV_NAME = 'fall_no_optional_cols.csv'
        preprocessor = FALLPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fall = FALLVerifier(self.PATH_TMP)
        fall.check_column_names_of_csv()
        FALLPreprocessor.CSV_NAME = 'fall.csv'
        FALLVerifier.CSV_NAME = 'fall.csv'

    def test_get_ids_of_valid_fall_cases(self):
        preprocessor = FALLPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fall = FALLVerifier(self.PATH_TMP)
        set_valid = set(fall.get_unique_ids_of_valid_encounter())
        set_all_ids = get_all_case_ids_as_set(fall)
        set_invalid = set_all_ids.difference(set_valid)
        set_required_invalid = {'1021', '1022', '1023'}
        self.assertEqual(set_required_invalid, set_invalid)

    def test_get_ids_of_valid_fall_cases_in_empty_df(self):
        FALLPreprocessor.CSV_NAME = 'fall_empty.csv'
        FALLVerifier.CSV_NAME = 'fall_empty.csv'
        preprocessor = FALLPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fall = FALLVerifier(self.PATH_TMP)
        with self.assertRaises(SystemExit):
            _ = set(fall.get_unique_ids_of_valid_encounter())
        FALLPreprocessor.CSV_NAME = 'fall.csv'
        FALLVerifier.CSV_NAME = 'fall.csv'

    def test_clear_invalid_fields_in_ikderkrankenkasse(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'ikderkrankenkasse': ['161556856', 'ABCDEFGH', 'ABCD-1234', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'ikderkrankenkasse')
        self.assertEqual(4, len(chunk['ikderkrankenkasse']))
        self.assertEqual(2, (len(chunk[chunk['ikderkrankenkasse'] == ''])))

    def test_clear_invalid_fields_in_geburtsjahr(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'geburtsjahr': ['1920', '2070', 'ABCD', 'BC2000', '14AD', '1899', '2106', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'geburtsjahr')
        self.assertEqual(8, len(chunk['geburtsjahr']))
        self.assertEqual(6, (len(chunk[chunk['geburtsjahr'] == ''])))

    def test_clear_invalid_fields_in_geschlecht(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'geschlecht': ['m', 'w', 'd', 'x', 'y', 'M', '1', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'geschlecht')
        self.assertEqual(8, len(chunk['geschlecht']))
        self.assertEqual(4, (len(chunk[chunk['geschlecht'] == ''])))

    def test_clear_invalid_fields_in_plz(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'plz': ['12345', 'ABCDE', '123456', '1234', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'plz')
        self.assertEqual(5, len(chunk['plz']))
        self.assertEqual(4, (len(chunk[chunk['plz'] == ''])))

    def test_clear_invalid_fields_in_aufnahmedatum(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'aufnahmedatum': ['202001010000', '202031122359', '20200101', 'MISSING', '202001010000.0000', '2020010100000000', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'aufnahmedatum')
        self.assertEqual(2, len(chunk['aufnahmedatum']))

    def test_clear_invalid_fields_in_aufnahmegrund(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'aufnahmegrund': ['0110', '0210', '0310', '0410', '0510', '0610', '0710', '0810', '0909', '1010', '110', 'A100', '10B0', 'ABCD', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'aufnahmegrund')
        self.assertEqual(10, len(chunk['aufnahmegrund']))

    def test_clear_invalid_fields_in_aufnahmeanlass(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'aufnahmeanlass': ['E', 'Z', 'N', 'R', 'V', 'A', 'G', 'B', ' A', 'A1', 'W', 'X', '1', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'aufnahmeanlass')
        self.assertEqual(8, len(chunk['aufnahmeanlass']))

    def test_clear_invalid_fields_in_fallzusammenfuehrung(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'fallzusammenführung': ['N', 'J', 'Y', 'Yes', 'No', '1', '0', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'fallzusammenführung')
        self.assertEqual(8, len(chunk['fallzusammenführung']))
        self.assertEqual(6, (len(chunk[chunk['fallzusammenführung'] == ''])))

    def test_clear_invalid_fields_in_fallzusammenfuehrungsgrund(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'fallzusammenführungsgrund': ['OG', 'MD', 'KO', 'RU', 'WR', 'MF', 'PW', 'PR', 'PM', 'ZO', 'ZM', 'ZK', 'ZR', 'ZW', 'Z0', 'OGO', '12', 'AC', 'MD1', 'D2', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'fallzusammenführungsgrund')
        self.assertEqual(21, len(chunk['fallzusammenführungsgrund']))
        self.assertEqual(5, (len(chunk[chunk['fallzusammenführungsgrund'] == ''])))

    def test_clear_invalid_fields_in_verweildauerintensiv(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'verweildauerintensiv': ['1', '500', '1,1', '10.1', '1,12', '10.12', '1,123', '10.123', 'A', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'verweildauerintensiv')
        self.assertEqual(10, len(chunk['verweildauerintensiv']))
        self.assertEqual(7, (len(chunk[chunk['verweildauerintensiv'] == ''])))

    def test_clear_invalid_fields_in_entlassungsdatum(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'entlassungsdatum': ['202001010000', '202031122359', '20200101', 'MISSING', '202001010000.0000', '2020010100000000', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'entlassungsdatum')
        self.assertEqual(7, len(chunk['entlassungsdatum']))
        self.assertEqual(5, (len(chunk[chunk['entlassungsdatum'] == ''])))

    def test_clear_invalid_fields_in_entlassungsgrund(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'entlassungsgrund': ['101', '10x', 'a10', 'abc', '20da', '12', 'aa', '1.1', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'entlassungsgrund')
        self.assertEqual(9, len(chunk['entlassungsgrund']))
        self.assertEqual(7, (len(chunk[chunk['entlassungsgrund'] == ''])))

    def test_clear_invalid_fields_in_beatmungsstunden(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'beatmungsstunden': ['1', '500', '1,1', '10.1', '1,12', '10.12', '1,123', '10.123', 'A', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'beatmungsstunden')
        self.assertEqual(10, len(chunk['beatmungsstunden']))
        self.assertEqual(7, (len(chunk[chunk['beatmungsstunden'] == ''])))

    def test_clear_invalid_fields_in_behandlungsbeginnvorstationaer(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'behandlungsbeginnvorstationär': ['202001010000', '202031122359', '20200101', 'MISSING', '202001010000.0000', '2020010100000000', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'behandlungsbeginnvorstationär')
        self.assertEqual(7, len(chunk['behandlungsbeginnvorstationär']))
        self.assertEqual(6, (len(chunk[chunk['behandlungsbeginnvorstationär'] == ''])))

    def test_clear_invalid_fields_in_behandlungstagevorstationaer(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'behandlungstagevorstationär': ['1', '10', '5000', 'AB', '12.1', '12,1', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'behandlungstagevorstationär')
        self.assertEqual(7, len(chunk['behandlungstagevorstationär']))
        self.assertEqual(4, (len(chunk[chunk['behandlungstagevorstationär'] == ''])))

    def test_clear_invalid_fields_in_behandlungsendenachstationaer(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'behandlungsendenachstationär': ['202001010000', '202031122359', '20200101', 'MISSING', '202001010000.0000', '2020010100000000', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'behandlungsendenachstationär')
        self.assertEqual(7, len(chunk['behandlungsendenachstationär']))
        self.assertEqual(6, (len(chunk[chunk['behandlungsendenachstationär'] == ''])))

    def test_clear_invalid_fields_in_behandlungstagenachstationaer(self):
        fall = FALLVerifier(self.PATH_TMP)
        chunk = pd.DataFrame({'behandlungstagenachstationär': ['1', '10', '5000', 'AB', '12.1', '12,1', '']})
        chunk = fall.clear_invalid_column_fields_in_chunk(chunk, 'behandlungstagenachstationär')
        self.assertEqual(7, len(chunk['behandlungstagenachstationär']))
        self.assertEqual(4, (len(chunk[chunk['behandlungstagenachstationär'] == ''])))

    def test_count_total_encounter(self):
        fall = FALLVerifier(self.PATH_TMP)
        num_cases = fall.count_total_encounter()
        self.assertEqual(4000, num_cases)

    def test_get_admission_dates_of_valid_cases(self):
        preprocessor = FALLPreprocessor(self.PATH_TMP)
        preprocessor.preprocess()
        fall = FALLVerifier(self.PATH_TMP)
        dict_admission_dates = fall.get_unique_ids_of_valid_encounter_with_admission_dates()
        self.assertEqual(3997, len(dict_admission_dates.values()))
        self.assertEqual(3997, len(dict_admission_dates.keys()))

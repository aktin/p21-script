import os
import unittest

import chardet
import pandas as pd

from src.p21import import CSVPreprocessor, FABPreprocessor, FALLPreprocessor, ICDPreprocessor, OPSPreprocessor
from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor


def get_csv_encoding(path_csv: str) -> str:
    with open(path_csv, 'rb') as csv:
        encoding = chardet.detect(csv.read(1024))['encoding']
    return encoding


def read_csv_header(preprocessor: CSVPreprocessor) -> str:
    df = pd.read_csv(preprocessor.PATH_CSV, nrows=0, index_col=None, sep=preprocessor.CSV_SEPARATOR, encoding=get_csv_encoding(preprocessor.PATH_CSV), dtype=str)
    return ';'.join(df.columns)


def count_unique_value_length_in_column(preprocessor: CSVPreprocessor, column: str) -> list:
    df = pd.read_csv(preprocessor.PATH_CSV, index_col=None, sep=preprocessor.CSV_SEPARATOR, encoding=get_csv_encoding(preprocessor.PATH_CSV), dtype=str)
    return df[column].apply(len).unique()


def count_values_with_given_length_in_column(preprocessor: CSVPreprocessor, column: str, length: int) -> int:
    df = pd.read_csv(preprocessor.PATH_CSV, index_col=None, sep=preprocessor.CSV_SEPARATOR, encoding=get_csv_encoding(preprocessor.PATH_CSV), dtype=str)
    df[column] = df[column].apply(lambda x: True if len(x) == length else False)
    return df[column].sum()


def count_rows_in_column(preprocessor: CSVPreprocessor, column: str) -> int:
    df = pd.read_csv(preprocessor.PATH_CSV, index_col=None, sep=preprocessor.CSV_SEPARATOR, encoding=get_csv_encoding(preprocessor.PATH_CSV), dtype=str)
    return df[column].shape[0]


class TestCSVPreprocessor(unittest.TestCase):

    def setUp(self):
        path_parent = os.path.dirname(os.getcwd())
        path_resources = os.path.join(path_parent, 'resources')
        path_zip = os.path.join(path_resources, 'p21_preprocess.zip')
        self.TMP = TmpFolderManager(path_resources)
        zfe = ZipFileExtractor(path_zip)
        self.PATH_TMP = self.TMP.create_tmp_folder()
        zfe.extract_zip_to_folder(self.PATH_TMP)

    def tearDown(self):
        self.TMP.remove_tmp_folder()

    def test_preprocess_FALL(self):
        fall = FALLPreprocessor(self.PATH_TMP)
        fall.preprocess()
        header = read_csv_header(fall)
        columns_csv = set(header.split(';'))
        columns_required = {'ik', 'entlassenderstandort', 'entgeltbereich', 'khinterneskennzeichen', 'versichertenid', 'vertragskennzeichen64bmodellvorhaben', 'ikderkrankenkasse',
                            'geburtsjahr', 'geburtsmonat', 'geschlecht', 'plz', 'wohnort', 'aufnahmedatum', 'aufnahmeanlass', 'aufnahmegrund', 'fallzusammenführung',
                            'fallzusammenführungsgrund', 'aufnahmegewicht', 'entlassungsdatum', 'entlassungsgrund', 'alterintagenamaufnahmetag', 'alterinjahrenamaufnahmetag',
                            'patientennummer', 'interkurrentedialysen', 'beatmungsstunden', 'behandlungsbeginnvorstationär', 'behandlungstagevorstationär',
                            'behandlungsendenachstationär', 'behandlungstagenachstationär', 'ikverlegungskh', 'belegungstageinanderementgeltbereich', 'beurlaubungstagepsy',
                            'kennungbesondererfallmodellvorhaben', 'verweildauerintensiv'}
        columns_matched = columns_required.intersection(columns_csv)
        self.assertTrue(columns_matched == columns_required)

    def test_preprocess_FAB(self):
        fab = FABPreprocessor(self.PATH_TMP)
        fab.preprocess()
        header = read_csv_header(fab)
        columns_csv = set(header.split(';'))
        columns_required = {'ik', 'entlassenderstandort', 'entgeltbereich', 'khinterneskennzeichen', 'standortnummerbehandlungsort', 'fachabteilung', 'fabaufnahmedatum',
                            'fabentlassungsdatum', 'kennungintensivbett'}
        columns_matched = columns_required.intersection(columns_csv)
        self.assertTrue(columns_matched == columns_required)

    def test_preprocess_FAB_alt(self):
        FABPreprocessor.CSV_NAME = 'fab_alt.csv'
        fab = FABPreprocessor(self.PATH_TMP)
        fab.preprocess()
        header = read_csv_header(fab)
        columns_csv = set(header.split(';'))
        columns_required = {'ik', 'entlassenderstandort', 'entgeltbereich', 'khinterneskennzeichen', 'standortnummerbehandlungsort', 'fachabteilung', 'fabaufnahmedatum',
                            'fabentlassungsdatum', 'kennungintensivbett'}
        columns_matched = columns_required.intersection(columns_csv)
        self.assertTrue(columns_matched == columns_required)
        FABPreprocessor.CSV_NAME = 'fab.csv'

    def test_preprocess_ICD(self):
        icd = ICDPreprocessor(self.PATH_TMP)
        icd.preprocess()
        header = read_csv_header(icd)
        columns_csv = set(header.split(';'))
        columns_required = {'ik', 'entlassenderstandort', 'entgeltbereich', 'khinterneskennzeichen', 'diagnoseart', 'icdversion', 'icdkode', 'lokalisation', 'diagnosensicherheit',
                            'sekundärkode', 'sekundärlokalisation', 'sekundärdiagnosensicherheit'}
        columns_matched = columns_required.intersection(columns_csv)
        self.assertTrue(columns_matched == columns_required)

    def test_preprocess_ICD_error(self):
        ICDPreprocessor.CSV_NAME = 'icd_error.csv'
        icd = ICDPreprocessor(self.PATH_TMP)
        with self.assertRaises(SystemExit):
            icd.preprocess()
        ICDPreprocessor.CSV_NAME = 'icd.csv'

    def test_preprocess_ICD_with_sek(self):
        ICDPreprocessor.CSV_NAME = 'icd_with_sek.csv'
        icd = ICDPreprocessor(self.PATH_TMP)
        icd.preprocess()
        header = read_csv_header(icd)
        columns_csv = set(header.split(';'))
        columns_required = {'ik', 'entlassenderstandort', 'entgeltbereich', 'khinterneskennzeichen', 'diagnoseart', 'icdversion', 'icdkode', 'lokalisation', 'diagnosensicherheit',
                            'sekundärkode', 'sekundärlokalisation', 'sekundärdiagnosensicherheit'}
        columns_matched = columns_required.intersection(columns_csv)
        self.assertTrue(columns_matched == columns_required)
        ICDPreprocessor.CSV_NAME = 'icd.csv'

    def test_preprocess_ICD_no_sek(self):
        ICDPreprocessor.CSV_NAME = 'icd_no_sek.csv'
        icd = ICDPreprocessor(self.PATH_TMP)
        icd.preprocess()
        header = read_csv_header(icd)
        columns_csv = set(header.split(';'))
        columns_required = {'ik', 'entlassenderstandort', 'entgeltbereich', 'khinterneskennzeichen', 'diagnoseart', 'icdversion', 'icdkode', 'lokalisation', 'diagnosensicherheit',
                            'sekundärkode', 'sekundärlokalisation', 'sekundärdiagnosensicherheit'}
        columns_matched = columns_required.intersection(columns_csv)
        self.assertTrue(columns_matched == columns_required)
        ICDPreprocessor.CSV_NAME = 'icd.csv'

    def test_preprocess_OPS(self):
        ops = OPSPreprocessor(self.PATH_TMP)
        ops.preprocess()
        header = read_csv_header(ops)
        columns_csv = set(header.split(';'))
        columns_required = {'ik', 'entlassenderstandort', 'entgeltbereich', 'khinterneskennzeichen', 'opsversion', 'opskode', 'lokalisation', 'opsdatum', 'belegoperateur',
                            'beleganästhesist', 'beleghebamme'}
        columns_matched = columns_required.intersection(columns_csv)
        self.assertTrue(columns_matched == columns_required)

    def test_appending_zeros_to_internal_id_FALL(self):
        FALLPreprocessor.LEADING_ZEROS = 4
        fall = FALLPreprocessor(self.PATH_TMP)
        count_rows_old = count_rows_in_column(fall, 'KH-internes-Kennzeichen')
        lengths_old = count_unique_value_length_in_column(fall, 'KH-internes-Kennzeichen')
        lengths_expected = [x + FALLPreprocessor.LEADING_ZEROS for x in lengths_old]
        fall.preprocess()
        lengths_new = count_unique_value_length_in_column(fall, 'khinterneskennzeichen')
        count_rows_new = count_rows_in_column(fall, 'khinterneskennzeichen')
        self.assertCountEqual(lengths_expected, lengths_new)
        self.assertEqual(count_rows_old, count_rows_new)
        FALLPreprocessor.LEADING_ZEROS = 0

    def test_appending_zeros_to_missing_internal_id_FALL_with_custom_chunk_size(self):
        FALLPreprocessor.LEADING_ZEROS = 4
        FALLPreprocessor.SIZE_CHUNKS = 10
        FALLPreprocessor.CSV_NAME = 'FALL_empty_internal_ids.csv'
        fall = FALLPreprocessor(self.PATH_TMP)
        fall.preprocess()
        lengths = count_unique_value_length_in_column(fall, 'khinterneskennzeichen')
        self.assertEqual(1, len(lengths))
        self.assertEqual(4, lengths[0])
        FALLPreprocessor.LEADING_ZEROS = 0

    def test_appending_zeros_to_internal_id_FAB_with_custom_chunk_size(self):
        FABPreprocessor.LEADING_ZEROS = 8
        FABPreprocessor.SIZE_CHUNKS = 10
        fab = FABPreprocessor(self.PATH_TMP)
        count_rows_old = count_rows_in_column(fab, 'KH-internes-Kennzeichen')
        lengths_old = count_unique_value_length_in_column(fab, 'KH-internes-Kennzeichen')
        lengths_expected = [x + FABPreprocessor.LEADING_ZEROS for x in lengths_old]
        fab.preprocess()
        lengths_new = count_unique_value_length_in_column(fab, 'khinterneskennzeichen')
        count_rows_new = count_rows_in_column(fab, 'khinterneskennzeichen')
        self.assertCountEqual(lengths_expected, lengths_new)
        self.assertEqual(count_rows_old, count_rows_new)
        FABPreprocessor.LEADING_ZEROS = 0

    def test_appending_zeros_to_internal_id_ICD_with_custom_chunk_size(self):
        ICDPreprocessor.LEADING_ZEROS = 1
        ICDPreprocessor.SIZE_CHUNKS = 10
        icd = ICDPreprocessor(self.PATH_TMP)
        count_rows_old = count_rows_in_column(icd, 'KH-internes-Kennzeichen')
        lengths_old = count_unique_value_length_in_column(icd, 'KH-internes-Kennzeichen')
        lengths_expected = [x + ICDPreprocessor.LEADING_ZEROS for x in lengths_old]
        icd.preprocess()
        lengths_new = count_unique_value_length_in_column(icd, 'khinterneskennzeichen')
        count_rows_new = count_rows_in_column(icd, 'khinterneskennzeichen')
        self.assertCountEqual(lengths_expected, lengths_new)
        self.assertEqual(count_rows_old, count_rows_new)
        ICDPreprocessor.LEADING_ZEROS = 0

    def test_appending_zeros_to_internal_id_OPS_with_custom_chunk_size(self):
        OPSPreprocessor.LEADING_ZEROS = -1
        OPSPreprocessor.SIZE_CHUNKS = 10
        ops = OPSPreprocessor(self.PATH_TMP)
        count_rows_old = count_rows_in_column(ops, 'KH-internes-Kennzeichen')
        lengths_expected = count_unique_value_length_in_column(ops, 'KH-internes-Kennzeichen')
        ops.preprocess()
        lengths_new = count_unique_value_length_in_column(ops, 'khinterneskennzeichen')
        count_rows_new = count_rows_in_column(ops, 'khinterneskennzeichen')
        self.assertCountEqual(lengths_expected, lengths_new)
        self.assertEqual(count_rows_old, count_rows_new)
        OPSPreprocessor.LEADING_ZEROS = 0

    def test_adding_of_leading_zeros_in_plz(self):
        FALLPreprocessor.CSV_NAME = 'FALL_missing_zeros.csv'
        fall = FALLPreprocessor(self.PATH_TMP)
        lengths_five_old = count_values_with_given_length_in_column(fall, 'PLZ', 5)
        lengths_four_old = count_values_with_given_length_in_column(fall, 'PLZ', 4)
        fall.preprocess()
        lengths_five_new = count_values_with_given_length_in_column(fall, 'plz', 5)
        lengths_four_new = count_values_with_given_length_in_column(fall, 'plz', 4)
        self.assertEqual(lengths_four_old + lengths_five_old, lengths_five_new)
        self.assertNotEqual(0, lengths_five_new)
        self.assertEqual(0, lengths_four_new)

    def test_adding_of_leading_zeros_in_aufnahmegrund(self):
        FALLPreprocessor.CSV_NAME = 'FALL_missing_zeros.csv'
        fall = FALLPreprocessor(self.PATH_TMP)
        lengths_four_old = count_values_with_given_length_in_column(fall, 'Aufnahmegrund', 4)
        lengths_three_old = count_values_with_given_length_in_column(fall, 'Aufnahmegrund', 3)
        fall.preprocess()
        lengths_four_new = count_values_with_given_length_in_column(fall, 'aufnahmegrund', 4)
        lengths_three_new = count_values_with_given_length_in_column(fall, 'aufnahmegrund', 3)
        self.assertEqual(lengths_three_old + lengths_four_old, lengths_four_new)
        self.assertNotEqual(0, lengths_four_new)
        self.assertEqual(0, lengths_three_new)

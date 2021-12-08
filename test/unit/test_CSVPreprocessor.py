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

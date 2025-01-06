import os
import unittest
import pandas as pd

from src.p21import import OPSObservationFactConverter
from src.p21import import OPSPreprocessor
from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor


class TestICDObservationFactConverter(unittest.TestCase):

    def read_csv_as_df(self) -> pd.DataFrame:
        df = pd.read_csv(self.PREPROCESSOR.PATH_CSV, sep=self.PREPROCESSOR.CSV_SEPARATOR, encoding='utf-8', dtype=str)
        df = df.fillna('')
        return df

    def setUp(self) -> None:
        path_parent = os.path.dirname(os.getcwd())
        path_resources = os.path.join(path_parent, 'resources')
        path_zip = os.path.join(path_resources, 'p21_conversion.zip')
        self.TMP = TmpFolderManager(path_resources)
        zfe = ZipFileExtractor(path_zip)
        self.PATH_TMP = self.TMP.create_tmp_folder()
        zfe.extract_zip_to_folder(self.PATH_TMP)
        self.TMP.rename_files_in_tmp_folder_to_lowercase()
        os.environ['uuid'] = '3fc5b451-1111-2222-3333-a70bfc58fd1f'
        os.environ['script_id'] = 'test'
        os.environ['script_version'] = '1.0'
        OPSPreprocessor.CSV_NAME = 'ops_conv.csv'
        self.PREPROCESSOR = OPSPreprocessor(self.PATH_TMP)
        self.PREPROCESSOR.preprocess()
        self.CONVERTER = OPSObservationFactConverter()
        self.DF = self.read_csv_as_df()

    def tearDown(self) -> None:
        self.TMP.remove_tmp_folder()

    def test_create_icd_observation_fact_row(self):
        self.__test_pat1_row1()
        self.__test_pat1_row2()
        self.__test_pat1_row3()
        self.__test_pat1_row4_missing_localisation()
        self.__test_pat2_row1_missing_localisation()
        self.__test_pat2_row2_missing_localisation()
        self.__test_pat2_row3_missing_localisation()

    def __test_pat1_row1(self):
        csv_row = self.DF.iloc[0]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(3, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 1)
        self.__test_version_row(df.iloc[1].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 1, '2019')
        self.__test_localisation_row(df.iloc[2].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 1, 'B')

    def __test_pat1_row2(self):
        csv_row = self.DF.iloc[1]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(3, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 2)
        self.__test_version_row(df.iloc[1].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 2, '2019')
        self.__test_localisation_row(df.iloc[2].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 2, 'L')

    def __test_pat1_row3(self):
        csv_row = self.DF.iloc[2]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(3, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 3)
        self.__test_version_row(df.iloc[1].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 3, '2019')
        self.__test_localisation_row(df.iloc[2].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 3, 'R')

    def __test_pat1_row4_missing_localisation(self):
        csv_row = self.DF.iloc[3]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 4)
        self.__test_version_row(df.iloc[1].dropna(), 'OPS:9-649.22', '2020-01-01 00:00', 4, '2019')

    def __test_pat2_row1_missing_localisation(self):
        csv_row = self.DF.iloc[4]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'OPS:1-502.0', '2020-01-01 00:00', 1)
        self.__test_version_row(df.iloc[1].dropna(), 'OPS:1-502.0', '2020-01-01 00:00', 1, '2019')

    def __test_pat2_row2_missing_localisation(self):
        csv_row = self.DF.iloc[5]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'OPS:1-501', '2020-01-01 00:00', 2)
        self.__test_version_row(df.iloc[1].dropna(), 'OPS:1-501', '2020-01-01 00:00', 2, '2019')

    def __test_pat2_row3_missing_localisation(self):
        csv_row = self.DF.iloc[6]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'OPS:1-051', '2020-01-01 00:00', 3)
        self.__test_version_row(df.iloc[1].dropna(), 'OPS:1-051', '2020-01-01 00:00', 3, '2019')

    def __test_concept_row(self, row: pd.Series, expected_concept: str, expected_date: str, expected_instance: int):
        self.assertEqual(6, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual(expected_date, row['start_date'])
        self.assertEqual('@', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('@', row['valtype_cd'])
        self.assertEqual('@', row['valueflag_cd'])

    def __test_version_row(self, row: pd.Series, expected_concept: str, expected_date: str, expected_instance: int, expected_version: str):
        self.assertEqual(7, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual(expected_date, row['start_date'])
        self.assertEqual('cdVersion', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('N', row['valtype_cd'])
        self.assertEqual(expected_version, row['nval_num'])
        self.assertEqual('yyyy', row['units_cd'])

    def __test_localisation_row(self, row: pd.Series, expected_concept: str, expected_date: str, expected_instance: int, expected_localisation: str):
        self.assertEqual(6, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual(expected_date, row['start_date'])
        self.assertEqual('localisation', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('T', row['valtype_cd'])
        self.assertEqual(expected_localisation, row['tval_char'])

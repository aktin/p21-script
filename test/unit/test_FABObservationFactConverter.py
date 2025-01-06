import os
import unittest
import pandas as pd

from src.p21import import FABObservationFactConverter
from src.p21import import FABPreprocessor
from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor


class TestFABObservationFactConverter(unittest.TestCase):

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
        FABPreprocessor.CSV_NAME = 'fab_conv.csv'
        self.PREPROCESSOR = FABPreprocessor(self.PATH_TMP)
        self.PREPROCESSOR.preprocess()
        self.CONVERTER = FABObservationFactConverter()
        self.DF = self.read_csv_as_df()

    def tearDown(self) -> None:
        self.TMP.remove_tmp_folder()

    def test_create_fab_observation_fact_row(self):
        self.__test_pat1_row1()
        self.__test_pat1_row2_missing_discharge()
        self.__test_pat2_row1()
        self.__test_pat2_row2_without_discharge()
        self.__test_pat2_row3_without_discharge()

    def __test_pat1_row1(self):
        row = self.DF.iloc[0]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(1, len(df.index))
        self.assertEqual(7, len(df.columns))
        self.assertEqual('P21:DEP:CC', df['concept_cd'][0])
        self.assertEqual('2020-01-01 00:00', df['start_date'][0])
        self.assertEqual('@', df['modifier_cd'][0])
        self.assertEqual(1, df['instance_num'][0])
        self.assertEqual('T', df['valtype_cd'][0])
        self.assertEqual('HA0001', df['tval_char'][0])
        self.assertEqual('2021-01-01 00:00', df['end_date'][0])

    def __test_pat1_row2_missing_discharge(self):
        row = self.DF.iloc[1]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(1, len(df.index))
        self.assertEqual(7, len(df.columns))
        self.assertEqual('P21:DEP', df['concept_cd'][0])
        self.assertEqual('2022-01-01 00:00', df['start_date'][0])
        self.assertEqual('@', df['modifier_cd'][0])
        self.assertEqual(2, df['instance_num'][0])
        self.assertEqual('T', df['valtype_cd'][0])
        self.assertEqual('BE0001', df['tval_char'][0])
        self.assertEqual(None, df['end_date'][0])

    def __test_pat2_row1(self):
        row = self.DF.iloc[2]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(1, len(df.index))
        self.assertEqual(7, len(df.columns))
        self.assertEqual('P21:DEP:CC', df['concept_cd'][0])
        self.assertEqual('2020-01-01 00:00', df['start_date'][0])
        self.assertEqual('@', df['modifier_cd'][0])
        self.assertEqual(1, df['instance_num'][0])
        self.assertEqual('T', df['valtype_cd'][0])
        self.assertEqual('HA0001', df['tval_char'][0])
        self.assertEqual('2021-01-01 00:00', df['end_date'][0])

    def __test_pat2_row2_without_discharge(self):
        row = self.DF.iloc[3]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(1, len(df.index))
        self.assertEqual(7, len(df.columns))
        self.assertEqual('P21:DEP', df['concept_cd'][0])
        self.assertEqual('2022-01-01 00:00', df['start_date'][0])
        self.assertEqual('@', df['modifier_cd'][0])
        self.assertEqual(2, df['instance_num'][0])
        self.assertEqual('T', df['valtype_cd'][0])
        self.assertEqual('BE0001', df['tval_char'][0])
        self.assertEqual(None, df['end_date'][0])

    def __test_pat2_row3_without_discharge(self):
        row = self.DF.iloc[4]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(1, len(df.index))
        self.assertEqual(7, len(df.columns))
        self.assertEqual('P21:DEP', df['concept_cd'][0])
        self.assertEqual('2023-01-01 00:00', df['start_date'][0])
        self.assertEqual('@', df['modifier_cd'][0])
        self.assertEqual(3, df['instance_num'][0])
        self.assertEqual('T', df['valtype_cd'][0])
        self.assertEqual('BE0002', df['tval_char'][0])
        self.assertEqual(None, df['end_date'][0])

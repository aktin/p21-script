import os
import unittest
import pandas as pd

from src.p21import import ICDObservationFactConverter
from src.p21import import ICDPreprocessor
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
        ICDPreprocessor.CSV_NAME = 'icd_conv.csv'
        self.PREPROCESSOR = ICDPreprocessor(self.PATH_TMP)
        self.PREPROCESSOR.preprocess()
        self.CONVERTER = ICDObservationFactConverter()
        self.DF = self.read_csv_as_df()

    def tearDown(self) -> None:
        self.TMP.remove_tmp_folder()

    def test_create_icd_observation_fact_row(self):
        self.__test_pat1_row1()
        self.__test_pat1_row2_missing_localisation()
        self.__test_pat1_row3_missing_certainty()
        self.__test_pat1_row4_missing_both()
        self.__test_pat2_row1_with_sec_diag()
        self.__test_pat2_row2_with_sec_diag_missing_localisation()
        self.__test_pat2_row3_with_sec_diag_missing_certainty()

    def __test_pat1_row1(self):
        csv_row = self.DF.iloc[0]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(5, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'ICD10GM:F24.24', 1)
        self.__test_diagType_row(df.iloc[1].dropna(), 'ICD10GM:F24.24', 1, 'HD')
        self.__test_version_row(df.iloc[2].dropna(), 'ICD10GM:F24.24', 1, '2019')
        self.__test_localisation_row(df.iloc[3].dropna(), 'ICD10GM:F24.24', 1, 'L')
        self.__test_certainty_row(df.iloc[4].dropna(), 'ICD10GM:F24.24', 1, 'A')

    def __test_pat1_row2_missing_localisation(self):
        csv_row = self.DF.iloc[1]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(4, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'ICD10GM:G25.25', 2)
        self.__test_diagType_row(df.iloc[1].dropna(), 'ICD10GM:G25.25', 2, 'ND')
        self.__test_version_row(df.iloc[2].dropna(), 'ICD10GM:G25.25', 2, '2019')
        self.__test_certainty_row(df.iloc[3].dropna(), 'ICD10GM:G25.25', 2, 'Z')

    def __test_pat1_row3_missing_certainty(self):
        csv_row = self.DF.iloc[2]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(4, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'ICD10GM:J90', 3)
        self.__test_diagType_row(df.iloc[1].dropna(), 'ICD10GM:J90', 3, 'ND')
        self.__test_version_row(df.iloc[2].dropna(), 'ICD10GM:J90', 3, '2019')
        self.__test_localisation_row(df.iloc[3].dropna(), 'ICD10GM:J90', 3, 'R')

    def __test_pat1_row4_missing_both(self):
        csv_row = self.DF.iloc[3]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(3, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'ICD10GM:J21.', 4)
        self.__test_diagType_row(df.iloc[1].dropna(), 'ICD10GM:J21.', 4, 'ND')
        self.__test_version_row(df.iloc[2].dropna(), 'ICD10GM:J21.', 4, '2019')

    def __test_pat2_row1_with_sec_diag(self):
        csv_row = self.DF.iloc[4]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(9, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'ICD10GM:V97.33XD', 1)
        self.__test_diagType_row(df.iloc[1].dropna(), 'ICD10GM:V97.33XD', 1, 'HD')
        self.__test_version_row(df.iloc[2].dropna(), 'ICD10GM:V97.33XD', 1, '2019')
        self.__test_concept_row(df.iloc[3].dropna(), 'ICD10GM:A22.22', 2)
        self.__test_diagType_row(df.iloc[4].dropna(), 'ICD10GM:A22.22', 2, 'SD')
        self.__test_version_row(df.iloc[5].dropna(), 'ICD10GM:A22.22', 2, '2019')
        self.__test_localisation_row(df.iloc[6].dropna(), 'ICD10GM:A22.22', 2, 'B')
        self.__test_certainty_row(df.iloc[7].dropna(), 'ICD10GM:A22.22', 2, 'A')
        self.__test_secondary_diagnosis_row(df.iloc[8].dropna(), 'ICD10GM:A22.22', 2, 'ICD10GM:V97.33XD')

    def __test_pat2_row2_with_sec_diag_missing_localisation(self):
        csv_row = self.DF.iloc[5]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(8, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'ICD10GM:V00.01XD', 3)
        self.__test_diagType_row(df.iloc[1].dropna(), 'ICD10GM:V00.01XD', 3, 'HD')
        self.__test_version_row(df.iloc[2].dropna(), 'ICD10GM:V00.01XD', 3, '2019')
        self.__test_concept_row(df.iloc[3].dropna(), 'ICD10GM:B11.11', 4)
        self.__test_diagType_row(df.iloc[4].dropna(), 'ICD10GM:B11.11', 4, 'SD')
        self.__test_version_row(df.iloc[5].dropna(), 'ICD10GM:B11.11', 4, '2019')
        self.__test_certainty_row(df.iloc[6].dropna(), 'ICD10GM:B11.11', 4, 'Z')
        self.__test_secondary_diagnosis_row(df.iloc[7].dropna(), 'ICD10GM:B11.11', 4, 'ICD10GM:V00.01XD')

    def __test_pat2_row3_with_sec_diag_missing_certainty(self):
        csv_row = self.DF.iloc[6]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(csv_row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(8, len(df.index))
        self.__test_concept_row(df.iloc[0].dropna(), 'ICD10GM:Y93.D', 5)
        self.__test_diagType_row(df.iloc[1].dropna(), 'ICD10GM:Y93.D', 5, 'HD')
        self.__test_version_row(df.iloc[2].dropna(), 'ICD10GM:Y93.D', 5, '2019')
        self.__test_concept_row(df.iloc[3].dropna(), 'ICD10GM:C33.33', 6)
        self.__test_diagType_row(df.iloc[4].dropna(), 'ICD10GM:C33.33', 6, 'SD')
        self.__test_version_row(df.iloc[5].dropna(), 'ICD10GM:C33.33', 6, '2019')
        self.__test_localisation_row(df.iloc[6].dropna(), 'ICD10GM:C33.33', 6, 'L')
        self.__test_secondary_diagnosis_row(df.iloc[7].dropna(), 'ICD10GM:C33.33', 6, 'ICD10GM:Y93.D')

    def __test_concept_row(self, row: pd.Series, expected_concept: str, expected_instance: int):
        self.assertEqual(5, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual('@', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('@', row['valtype_cd'])
        self.assertEqual('@', row['valueflag_cd'])

    def __test_diagType_row(self, row: pd.Series, expected_concept: str, expected_instance: int, expected_diag: str):
        self.assertEqual(5, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual('diagType', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('T', row['valtype_cd'])
        self.assertEqual(expected_diag, row['tval_char'])

    def __test_version_row(self, row: pd.Series, expected_concept: str, expected_instance: int, expected_version: str):
        self.assertEqual(6, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual('cdVersion', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('N', row['valtype_cd'])
        self.assertEqual(expected_version, row['nval_num'])
        self.assertEqual('yyyy', row['units_cd'])

    def __test_localisation_row(self, row: pd.Series, expected_concept: str, expected_instance: int, expected_localisation: str):
        self.assertEqual(5, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual('localisation', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('T', row['valtype_cd'])
        self.assertEqual(expected_localisation, row['tval_char'])

    def __test_certainty_row(self, row: pd.Series, expected_concept: str, expected_instance: int, expected_certainty: str):
        self.assertEqual(5, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual('certainty', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('T', row['valtype_cd'])
        self.assertEqual(expected_certainty, row['tval_char'])

    def __test_secondary_diagnosis_row(self, row: pd.Series, expected_concept: str, expected_instance: int, expected_parent: str):
        self.assertEqual(5, len(row))
        self.assertEqual(expected_concept, row['concept_cd'])
        self.assertEqual('sdFrom', row['modifier_cd'])
        self.assertEqual(expected_instance, row['instance_num'])
        self.assertEqual('T', row['valtype_cd'])
        self.assertEqual(expected_parent, row['tval_char'])

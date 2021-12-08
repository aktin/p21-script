import os
import unittest

import chardet
import pandas as pd

from src.p21import import FALLObservationFactConverter
from src.p21import import FALLPreprocessor
from src.p21import import TmpFolderManager
from src.p21import import ZipFileExtractor


def get_csv_encoding(path_csv: str) -> str:
    with open(path_csv, 'rb') as csv:
        encoding = chardet.detect(csv.read(1024))['encoding']
    return encoding


def drop_nan_columns_in_row(row: pd.Series):
    return row[row.columns[~row.isnull().all()]]


class TestFALLObservationFactConverter(unittest.TestCase):

    def read_csv_as_df(self) -> pd.DataFrame:
        df = pd.read_csv(self.PREPROCESSOR.PATH_CSV, sep=self.PREPROCESSOR.CSV_SEPARATOR, encoding=get_csv_encoding(self.PREPROCESSOR.PATH_CSV), dtype=str)
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
        os.environ['uuid'] = '3fc5b451-1111-2222-3333-a70bfc58fd1f'
        os.environ['script_id'] = 'test'
        os.environ['script_version'] = '1.0'
        FALLPreprocessor.CSV_NAME = 'fall_conv.csv'
        self.PREPROCESSOR = FALLPreprocessor(self.PATH_TMP)
        self.PREPROCESSOR.preprocess()
        self.CONVERTER = FALLObservationFactConverter()
        self.DF = self.read_csv_as_df()

    def tearDown(self) -> None:
        self.TMP.remove_tmp_folder()

    def test_create_script_rows(self):
        list_script_rows = self.CONVERTER.create_script_rows()
        df = pd.DataFrame(list_script_rows)
        rows_script = df[df['concept_cd'] == 'P21:SCRIPT']
        row1 = rows_script.iloc[0]
        self.assertEqual('@', row1['modifier_cd'])
        self.assertEqual('@', row1['valtype_cd'])
        self.assertEqual('@', row1['valueflag_cd'])
        row2 = rows_script.iloc[1]
        self.assertEqual('scriptVer', row2['modifier_cd'])
        self.assertEqual('T', row2['valtype_cd'])
        self.assertEqual(os.environ['script_version'], row2['tval_char'])
        row3 = rows_script.iloc[2]
        self.assertEqual('scriptId', row3['modifier_cd'])
        self.assertEqual('T', row3['valtype_cd'])
        self.assertEqual(os.environ['script_id'], row3['tval_char'])

    def test_create_row_max(self):
        row = self.DF.iloc[0]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(13, len(df.index))
        self.assertTrue(self.__check_admission_row(df))
        self.assertTrue(self.__check_insurance_row(df))
        self.assertTrue(self.__check_birthyear_row(df))
        self.assertTrue(self.__check_sex_row(df))
        self.assertTrue(self.__check_zipcode_row(df))
        self.assertTrue(self.__check_encounter_merge_row(df))
        self.assertTrue(self.__check_critical_care_row(df))
        self.assertTrue(self.__check_discharge_row(df))
        self.assertTrue(self.__check_ventilation_row(df))
        self.assertTrue(self.__check_prestation_row(df))
        self.assertTrue(self.__check_poststation_row(df))

    def test_create_row_min(self):
        row = self.DF.iloc[1]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.assertTrue(self.__check_admission_row(df))
        self.assertFalse(self.__check_insurance_row(df))
        self.assertFalse(self.__check_birthyear_row(df))
        self.assertFalse(self.__check_sex_row(df))
        self.assertFalse(self.__check_zipcode_row(df))
        self.assertFalse(self.__check_encounter_merge_row(df))
        self.assertFalse(self.__check_critical_care_row(df))
        self.assertFalse(self.__check_discharge_row(df))
        self.assertFalse(self.__check_ventilation_row(df))
        self.assertFalse(self.__check_prestation_row(df))
        self.assertFalse(self.__check_poststation_row(df))

    def test_create_row_no_merge(self):
        row = self.DF.iloc[2]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.assertTrue(self.__check_admission_row(df))
        self.assertFalse(self.__check_insurance_row(df))
        self.assertFalse(self.__check_birthyear_row(df))
        self.assertFalse(self.__check_sex_row(df))
        self.assertFalse(self.__check_zipcode_row(df))
        self.assertFalse(self.__check_encounter_merge_row(df))
        self.assertFalse(self.__check_critical_care_row(df))
        self.assertFalse(self.__check_discharge_row(df))
        self.assertFalse(self.__check_ventilation_row(df))
        self.assertFalse(self.__check_prestation_row(df))
        self.assertFalse(self.__check_poststation_row(df))

    def test_create_row_missing_merge(self):
        row = self.DF.iloc[3]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.assertTrue(self.__check_admission_row(df))
        self.assertFalse(self.__check_insurance_row(df))
        self.assertFalse(self.__check_birthyear_row(df))
        self.assertFalse(self.__check_sex_row(df))
        self.assertFalse(self.__check_zipcode_row(df))
        self.assertFalse(self.__check_encounter_merge_row(df))
        self.assertFalse(self.__check_critical_care_row(df))
        self.assertFalse(self.__check_discharge_row(df))
        self.assertFalse(self.__check_ventilation_row(df))
        self.assertFalse(self.__check_prestation_row(df))
        self.assertFalse(self.__check_poststation_row(df))

    def test_create_row_missing_discharge1(self):
        row = self.DF.iloc[4]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.assertTrue(self.__check_admission_row(df))
        self.assertFalse(self.__check_insurance_row(df))
        self.assertFalse(self.__check_birthyear_row(df))
        self.assertFalse(self.__check_sex_row(df))
        self.assertFalse(self.__check_zipcode_row(df))
        self.assertFalse(self.__check_encounter_merge_row(df))
        self.assertFalse(self.__check_critical_care_row(df))
        self.assertFalse(self.__check_discharge_row(df))
        self.assertFalse(self.__check_ventilation_row(df))
        self.assertFalse(self.__check_prestation_row(df))
        self.assertFalse(self.__check_poststation_row(df))

    def test_create_row_missing_discharge2(self):
        row = self.DF.iloc[5]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(2, len(df.index))
        self.assertTrue(self.__check_admission_row(df))
        self.assertFalse(self.__check_insurance_row(df))
        self.assertFalse(self.__check_birthyear_row(df))
        self.assertFalse(self.__check_sex_row(df))
        self.assertFalse(self.__check_zipcode_row(df))
        self.assertFalse(self.__check_encounter_merge_row(df))
        self.assertFalse(self.__check_critical_care_row(df))
        self.assertFalse(self.__check_discharge_row(df))
        self.assertFalse(self.__check_ventilation_row(df))
        self.assertFalse(self.__check_prestation_row(df))
        self.assertFalse(self.__check_poststation_row(df))

    def test_create_row_missing_pre_post_days(self):
        row = self.DF.iloc[6]
        list_observation_fact_dicts = self.CONVERTER.create_observation_facts_from_row(row)
        df = pd.DataFrame(list_observation_fact_dicts)
        self.assertEqual(4, len(df.index))
        self.assertTrue(self.__check_admission_row(df))
        self.assertFalse(self.__check_insurance_row(df))
        self.assertFalse(self.__check_birthyear_row(df))
        self.assertFalse(self.__check_sex_row(df))
        self.assertFalse(self.__check_zipcode_row(df))
        self.assertFalse(self.__check_encounter_merge_row(df))
        self.assertFalse(self.__check_critical_care_row(df))
        self.assertFalse(self.__check_discharge_row(df))
        self.assertFalse(self.__check_ventilation_row(df))
        self.assertTrue(self.__check_prestation_row_without_days(df))
        self.assertTrue(self.__check_poststation_row_without_days(df))

    def __check_admission_row(self, df: pd.DataFrame) -> bool:
        row_cause = df[df['concept_cd'] == 'P21:ADMC:N']
        if row_cause.empty:
            return False
        row_cause = drop_nan_columns_in_row(row_cause)
        self.assertEqual(['@'], row_cause['modifier_cd'].values)
        self.assertEqual(['@'], row_cause['valtype_cd'].values)
        self.assertEqual(['@'], row_cause['valueflag_cd'].values)
        self.assertEqual(4, len(row_cause.columns))
        row_reason = df[df['concept_cd'] == 'P21:ADMR:1010']
        if row_reason.empty:
            return False
        row_reason = drop_nan_columns_in_row(row_reason)
        self.assertEqual(['@'], row_reason['modifier_cd'].values)
        self.assertEqual(['@'], row_reason['valtype_cd'].values)
        self.assertEqual(['@'], row_reason['valueflag_cd'].values)
        self.assertEqual(4, len(row_reason.columns))
        return True

    def __check_insurance_row(self, df: pd.DataFrame) -> bool:
        row_insurance = df[df['concept_cd'] == 'AKTIN:IKNR']
        if row_insurance.empty:
            return False
        row_insurance = drop_nan_columns_in_row(row_insurance)
        self.assertEqual(['@'], row_insurance['modifier_cd'].values)
        self.assertEqual(['T'], row_insurance['valtype_cd'].values)
        self.assertEqual(['161556856'], row_insurance['tval_char'].values)
        self.assertEqual(4, len(row_insurance.columns))
        return True

    def __check_birthyear_row(self, df: pd.DataFrame) -> bool:
        rows_birthyear = df[df['concept_cd'] == 'LOINC:80904-6']
        if rows_birthyear.empty:
            return False
        rows_birthyear = drop_nan_columns_in_row(rows_birthyear)
        row1 = rows_birthyear.iloc[0].dropna()
        self.assertEqual(5, len(row1))
        self.assertEqual('@', row1['modifier_cd'])
        self.assertEqual('N', row1['valtype_cd'])
        self.assertEqual('2000', row1['nval_num'])
        self.assertEqual('yyyy', row1['units_cd'])
        row2 = rows_birthyear.iloc[1].dropna()
        self.assertEqual(4, len(row2))
        self.assertEqual('effectiveTime', row2['modifier_cd'])
        self.assertEqual('T', row2['valtype_cd'])
        self.assertEqual('202001010000', row2['tval_char'])
        return True

    def __check_sex_row(self, df: pd.DataFrame) -> bool:
        row_sex = df[df['concept_cd'] == 'P21:SEX:M']
        if row_sex.empty:
            return False
        row_sex = drop_nan_columns_in_row(row_sex)
        self.assertEqual(['@'], row_sex['modifier_cd'].values)
        self.assertEqual(['@'], row_sex['valtype_cd'].values)
        self.assertEqual(['@'], row_sex['valueflag_cd'].values)
        self.assertEqual(4, len(row_sex.columns))
        return True

    def __check_zipcode_row(self, df: pd.DataFrame) -> bool:
        row_zipcode = df[df['concept_cd'] == 'AKTIN:ZIPCODE']
        if row_zipcode.empty:
            return False
        row_zipcode = drop_nan_columns_in_row(row_zipcode)
        self.assertEqual(['@'], row_zipcode['modifier_cd'].values)
        self.assertEqual(['T'], row_zipcode['valtype_cd'].values)
        self.assertEqual(['12345'], row_zipcode['tval_char'].values)
        self.assertEqual(4, len(row_zipcode.columns))
        return True

    def __check_encounter_merge_row(self, df: pd.DataFrame) -> bool:
        row_merge = df[df['concept_cd'] == 'P21:MERGE:OG']
        if row_merge.empty:
            return False
        row_merge = drop_nan_columns_in_row(row_merge)
        self.assertEqual(['@'], row_merge['modifier_cd'].values)
        self.assertEqual(['@'], row_merge['valtype_cd'].values)
        self.assertEqual(['@'], row_merge['valueflag_cd'].values)
        self.assertEqual(4, len(row_merge.columns))
        return True

    def __check_critical_care_row(self, df: pd.DataFrame) -> bool:
        row_critical_care = df[df['concept_cd'] == 'P21:DCC']
        if row_critical_care.empty:
            return False
        row_critical_care = drop_nan_columns_in_row(row_critical_care)
        self.assertEqual(['@'], row_critical_care['modifier_cd'].values)
        self.assertEqual(['N'], row_critical_care['valtype_cd'].values)
        self.assertEqual(['100.50'], row_critical_care['nval_num'].values)
        self.assertEqual(['d'], row_critical_care['units_cd'].values)
        self.assertEqual(5, len(row_critical_care.columns))
        return True

    def __check_discharge_row(self, df: pd.DataFrame) -> bool:
        row_discharge = df[df['concept_cd'] == 'P21:DISR:179']
        if row_discharge.empty:
            return False
        row_discharge = drop_nan_columns_in_row(row_discharge)
        self.assertEqual(['2021-01-01 00:00'], row_discharge['start_date'].values)
        self.assertEqual(['@'], row_discharge['modifier_cd'].values)
        self.assertEqual(['@'], row_discharge['valtype_cd'].values)
        self.assertEqual(['@'], row_discharge['valueflag_cd'].values)
        self.assertEqual(5, len(row_discharge.columns))
        return True

    def __check_ventilation_row(self, df: pd.DataFrame) -> bool:
        row_ventilation = df[df['concept_cd'] == 'P21:DV']
        if row_ventilation.empty:
            return False
        row_ventilation = drop_nan_columns_in_row(row_ventilation)
        self.assertEqual(['@'], row_ventilation['modifier_cd'].values)
        self.assertEqual(['N'], row_ventilation['valtype_cd'].values)
        self.assertEqual(['500.50'], row_ventilation['nval_num'].values)
        self.assertEqual(['h'], row_ventilation['units_cd'].values)
        self.assertEqual(5, len(row_ventilation.columns))
        return True

    def __check_prestation_row(self, df: pd.DataFrame) -> bool:
        row_prestation = df[df['concept_cd'] == 'P21:PREADM']
        if row_prestation.empty:
            return False
        row_prestation = drop_nan_columns_in_row(row_prestation)
        self.assertEqual(['2019-01-01 00:00'], row_prestation['start_date'].values)
        self.assertEqual(['@'], row_prestation['modifier_cd'].values)
        self.assertEqual(['N'], row_prestation['valtype_cd'].values)
        self.assertEqual(['365'], row_prestation['nval_num'].values)
        self.assertEqual(['d'], row_prestation['units_cd'].values)
        self.assertEqual(6, len(row_prestation.columns))
        return True

    def __check_prestation_row_without_days(self, df: pd.DataFrame) -> bool:
        row_prestation = df[df['concept_cd'] == 'P21:PREADM']
        if row_prestation.empty:
            return False
        row_prestation = drop_nan_columns_in_row(row_prestation)
        self.assertEqual(['2019-01-01 00:00'], row_prestation['start_date'].values)
        self.assertEqual(['@'], row_prestation['modifier_cd'].values)
        self.assertEqual(['@'], row_prestation['valtype_cd'].values)
        self.assertEqual(['@'], row_prestation['valueflag_cd'].values)
        self.assertEqual(5, len(row_prestation.columns))
        return True

    def __check_poststation_row(self, df: pd.DataFrame) -> bool:
        row_poststation = df[df['concept_cd'] == 'P21:POSTDIS']
        if row_poststation.empty:
            return False
        row_poststation = drop_nan_columns_in_row(row_poststation)
        self.assertEqual(['2022-01-01 00:00'], row_poststation['start_date'].values)
        self.assertEqual(['@'], row_poststation['modifier_cd'].values)
        self.assertEqual(['N'], row_poststation['valtype_cd'].values)
        self.assertEqual(['365'], row_poststation['nval_num'].values)
        self.assertEqual(['d'], row_poststation['units_cd'].values)
        self.assertEqual(6, len(row_poststation.columns))
        return True

    def __check_poststation_row_without_days(self, df: pd.DataFrame) -> bool:
        row_poststation = df[df['concept_cd'] == 'P21:POSTDIS']
        if row_poststation.empty:
            return False
        row_poststation = drop_nan_columns_in_row(row_poststation)
        self.assertEqual(['2022-01-01 00:00'], row_poststation['start_date'].values)
        self.assertEqual(['@'], row_poststation['modifier_cd'].values)
        self.assertEqual(['@'], row_poststation['valtype_cd'].values)
        self.assertEqual(['@'], row_poststation['valueflag_cd'].values)
        self.assertEqual(5, len(row_poststation.columns))
        return True

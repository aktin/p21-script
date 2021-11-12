import unittest
import os
from src.p21import import FALLObservationFactConverter


class TestCSVObservationFactConverter(unittest.TestCase):

    def setUp(self) -> None:
        os.environ['uuid'] = 'a70bfc58fd1f'
        os.environ['script_id'] = 'test'
        os.environ['script_version'] = '1.0'
        self.CSV = FALLObservationFactConverter()

    def test_add_static_values_to_empty_row_dict(self):
        row = {}
        row = self.CSV.add_static_values_to_row_dict(row, 'E5', 'P5', '202012310000')
        self.assertEqual('E5', row.get('encounter_num'))
        self.assertEqual('P5', row.get('patient_num'))
        self.assertEqual('P21', row.get('provider_id'))
        self.assertEqual('2020-12-31 00:00', row.get('start_date'))
        self.assertEqual(1, row.get('instance_num'))
        self.assertIsNone(row.get('tval_char'))
        self.assertIsNone(row.get('nval_num'))
        self.assertIsNone(row.get('valueflag_cd'))
        self.assertEqual('@', row.get('units_cd'))
        self.assertIsNone(row.get('end_date'))
        self.assertEqual('@', row.get('location_cd'))
        self.assertIsNotNone(row.get('import_date'))
        self.assertIsNotNone(row.get('update_date'))
        self.assertIsNotNone(row.get('download_date'))
        self.assertEqual('test_a70bfc58fd1f', row.get('sourcesystem_cd'))
        self.assertEqual(15, len(row.keys()))

    def test_add_static_values_to_full_row_dict(self):
        row = {'encounter_num': 'E100', 'patient_num': 'P100', 'provider_id': 'provider', 'start_date': '2022-01-01 00:00', 'instance_num': 5, 'tval_char': 'char', 'nval_num': 'num',
               'valueflag_cd': 'flag', 'units_cd': 'yyyy', 'end_date': '2025-01-01 00:00', 'location_cd': 'location', 'import_date': '2022-01-01 00:00', 'update_date': '2022-01-01 00:00',
               'download_date': '2022-01-01 00:00', 'sourcesystem_cd': 'aaaa-bbbb-cccc'}
        row = self.CSV.add_static_values_to_row_dict(row, 'E5', 'P5', '202012310000')
        self.assertEqual('E5', row.get('encounter_num'))
        self.assertEqual('P5', row.get('patient_num'))
        self.assertEqual('P21', row.get('provider_id'))
        self.assertEqual('2022-01-01 00:00', row.get('start_date'))
        self.assertEqual(5, row.get('instance_num'))
        self.assertEqual('char', row.get('tval_char'))
        self.assertEqual('num', row.get('nval_num'))
        self.assertEqual('flag', row.get('valueflag_cd'))
        self.assertEqual('yyyy', row.get('units_cd'))
        self.assertEqual('2025-01-01 00:00', row.get('end_date'))
        self.assertEqual('@', row.get('location_cd'))
        self.assertIsNotNone(row.get('import_date'))
        self.assertNotEqual('2022-01-01 00:00', row.get('import_date'))
        self.assertIsNotNone(row.get('update_date'))
        self.assertNotEqual('2022-01-01 00:00', row.get('update_date'))
        self.assertIsNotNone(row.get('download_date'))
        self.assertNotEqual('2022-01-01 00:00', row.get('download_date'))
        self.assertEqual('test_a70bfc58fd1f', row.get('sourcesystem_cd'))
        self.assertEqual(15, len(row.keys()))

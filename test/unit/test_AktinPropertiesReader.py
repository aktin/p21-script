import unittest
import os
from src.p21import import AktinPropertiesReader


class TestAktinPropertiesReader(unittest.TestCase):

    def setUp(self) -> None:
        path_parent = os.path.dirname(os.getcwd())
        path_resources = os.path.join(path_parent, 'resources')
        path_aktin_properties = os.path.join(path_resources, 'aktin.properties')
        os.environ['path_aktin_properties'] = path_aktin_properties
        self.READER = AktinPropertiesReader()

    def test_init_with_wrong_path(self):
        os.environ['path_aktin_properties'] = 'wrong/path'
        with self.assertRaises(SystemExit):
            _ = AktinPropertiesReader()

    def test_get_property_encounter_root(self):
        prop = self.READER.get_property('cda.encounter.root.preset')
        self.assertEqual('1.2.276.0.76.3.87686', prop)

    def test_get_property_billing_root(self):
        prop = self.READER.get_property('cda.billing.root.preset')
        self.assertEqual('1.2.276.0.76.3.87686.1.45', prop)

    def test_get_property_salt(self):
        prop = self.READER.get_property('pseudonym.salt')
        self.assertEqual('', prop)

    def test_get_missing_property(self):
        prop = self.READER.get_property('broker.url')
        self.assertEqual('', prop)

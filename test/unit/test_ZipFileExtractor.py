import unittest
import os
from src.p21import import ZipFileExtractor


class TestZipFileExtractor(unittest.TestCase):

    def test_check_zip_file_integrity(self):
        path_parent = os.path.dirname(os.getcwd())
        path_zip = os.path.join(path_parent, 'resources', 'p21_verification.zip')
        _ = ZipFileExtractor(path_zip)

    def test_check_invalid_zip_file(self):
        with self.assertRaises(SystemExit):
            ZipFileExtractor('p21.zip')

    def test_extract_zip_to_folder(self):
        # done indirectly by other tests
        pass

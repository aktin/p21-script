import unittest
import os
from src.p21import import TmpFolderManager


def write_basic_file_to_folder(path_folder: str):
    with open(os.path.join(path_folder, 'TEST'), 'wb') as temp_file:
        temp_file.write(b'test')


def does_tmp_folder_exist_in_folder(path_folder: str):
    return os.path.isdir(os.path.join(path_folder, 'tmp'))


class TestTmpFolderManager(unittest.TestCase):

    def setUp(self) -> None:
        path_parent = os.path.dirname(os.getcwd())
        self.PATH_RESOURCES = os.path.join(path_parent, 'resources')
        self.TMP = TmpFolderManager(self.PATH_RESOURCES)

    def tearDown(self) -> None:
        self.TMP.remove_tmp_folder()
        self.assertFalse(does_tmp_folder_exist_in_folder(self.PATH_RESOURCES))

    def test_create_and_delete_tmp_folder(self):
        self.assertFalse(does_tmp_folder_exist_in_folder(self.PATH_RESOURCES))
        self.TMP.create_tmp_folder()
        self.assertTrue(does_tmp_folder_exist_in_folder(self.PATH_RESOURCES))

    def test_rename_files_in_tmp_to_lowercase(self):
        path_tmp = self.TMP.create_tmp_folder()
        write_basic_file_to_folder(path_tmp)
        self.assertEqual(os.listdir(path_tmp), ['TEST'])
        self.TMP.rename_files_in_tmp_folder_to_lowercase()
        self.assertEqual(os.listdir(path_tmp), ['test'])
